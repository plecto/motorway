from Queue import Empty
import logging
import multiprocessing
import random
from threading import Thread
import time
from time import time as _time
import uuid
import datetime
from motorway.messages import Message
from motorway.mixins import GrouperMixin, SendMessageMixin
from motorway.utils import set_timeouts_on_socket, get_connections_block
import zmq
from setproctitle import setproctitle

logger = logging.getLogger(__name__)


class Intersection(GrouperMixin, SendMessageMixin, object):

    fields = []

    def __init__(self):
        self.messages_processed = 0
        self.process_uuid = str(uuid.uuid4())
        self.receive_port = None
        self.send_socks = {}
        self.send_grouper = None
        self.controller_sock = None
        self.message_batch_start = datetime.datetime.now()  # This is used to time how much time messages take

    def _process(self, receive_sock, controller_sock=None):

        try:
            if getattr(self.process, 'batch_process', None):
                poller = zmq.Poller()
                poller.register(receive_sock, zmq.POLLIN)
                value = []
                end_time = _time() + self.process.wait
                while end_time > _time() and len(value) < self.process.limit:
                    socks = dict(poller.poll(timeout=1000))
                    if socks.get(receive_sock) == zmq.POLLIN:
                        value.append(receive_sock.recv_json())
                message_count = len(value)
            else:
                poller = zmq.Poller()
                poller.register(receive_sock, zmq.POLLIN)
                socks = dict(poller.poll(timeout=1000))
                if socks.get(receive_sock) == zmq.POLLIN:
                    value = receive_sock.recv_json()
                else:
                    return
                message_count = 1
            if value:
                self.messages_processed += message_count
                if isinstance(value, list):
                    message = [Message.from_message(m, controller_sock, process_name=self.process_uuid) for m in value]
                else:
                    message = Message.from_message(value, controller_sock, process_name=self.process_uuid)
                try:
                    self.message_batch_start = datetime.datetime.now()
                    for generated_message in self.process(message):
                        if generated_message is not None and self.send_socks:
                            self.send_message(generated_message, self.process_uuid, time_consumed=(datetime.datetime.now() - self.message_batch_start))
                            self.message_batch_start = datetime.datetime.now()
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    if isinstance(message, list):
                        [m.fail() for m in message]
                    else:
                        message.fail()

        except Empty:  # Didn't receive anything from ZMQ
            pass

    def ack(self, message):
        message.ack(time_consumed=(datetime.datetime.now() - self.message_batch_start))
        self.message_batch_start = datetime.datetime.now()

    def fail(self, message, **kwargs):
        message.fail(**kwargs)

    def process(self, message):
        """
        This function is called continuously by the intersection.

        :yield: :class:`motorway.messages.Message` instance
        :param message: :class:`motorway.messages.Message` instance or :func:`list` if using
            :func:`motorway.decorators.batch_process`

        """
        raise NotImplementedError()

    @classmethod
    def run(cls, input_stream, output_stream=None, refresh_connection_stream=None, grouper_cls=None):
        self = cls()
        process_name = multiprocessing.current_process().name
        logger.info("Running %s" % process_name)
        logger.debug("%s listening on %s and pushing to %s" % (process_name, input_stream, output_stream))
        setproctitle("data-pipeline: %s" % process_name)
        context = zmq.Context()

        # Create Thread Factories :-)

        thread_update_connections_factory = lambda: Thread(target=self.connection_thread, name="connection_thread", kwargs={
            'refresh_connection_stream': refresh_connection_stream,
            'context': context,
            'input_queue': input_stream,
            'output_queue': output_stream,
            'process_id': self.process_uuid,
            'process_name': process_name,
            'grouper_cls': grouper_cls
        })

        thread_main_factory = lambda: Thread(target=self.receive_messages, name="message_producer", kwargs={
            'context': context,
            'output_stream': output_stream,
            'grouper_cls': grouper_cls,
        })

        # Run threads

        thread_update_connections = thread_update_connections_factory()
        thread_update_connections.start()

        thread_main = thread_main_factory()
        thread_main.start()

        while True:
            if not thread_update_connections.isAlive():
                logger.error("Thread thread_update_connections crashed in %s" % self.__class__.__name__)
                thread_update_connections = thread_update_connections_factory()
                thread_update_connections.start()
            if not thread_main.isAlive():
                logger.error("Thread thread_main crashed in %s" % self.__class__.__name__)
                thread_main = thread_main_factory()
                thread_main.start()
            time.sleep(10)

    def connection_thread(self, context=None, refresh_connection_stream=None, input_queue=None, output_queue=None, process_id=None,
                          process_name=None, grouper_cls=None):
        refresh_connection_sock = context.socket(zmq.SUB)
        refresh_connection_sock.connect(refresh_connection_stream)
        refresh_connection_sock.setsockopt(zmq.SUBSCRIBE, '')  # You must subscribe to something, so this means *all*}
        set_timeouts_on_socket(refresh_connection_sock)

        connections = get_connections_block('_update_connections', refresh_connection_sock)

        while not self.receive_port:
            time.sleep(1)
        # Register as consumer of input stream
        update_connection_sock = context.socket(zmq.PUSH)
        update_connection_sock.connect(connections['_update_connections']['streams'][0])
        intersection_connection_info = {
            'streams': {
                input_queue: ['tcp://127.0.0.1:%s' % self.receive_port]
            },
            'meta': {
                'id': process_id,
                'name': process_name,
                'grouping': None if not grouper_cls else grouper_cls.__name__  # Specify how messages sent to this intersection should be grouped
            }
        }
        update_connection_sock.send_json(intersection_connection_info)
        last_send_time = datetime.datetime.now()

        connections = get_connections_block('_message_ack', refresh_connection_sock, existing_connections=connections)
        self.controller_sock = context.socket(zmq.PUSH)
        self.controller_sock.connect(connections['_message_ack']['streams'][0])
        set_timeouts_on_socket(self.controller_sock)

        while True:
            try:
                connections = refresh_connection_sock.recv_json()
                if output_queue in connections:
                    self.set_send_socks(connections, output_queue, context)

            except zmq.Again:
                time.sleep(1)
            if last_send_time + datetime.timedelta(seconds=10) < datetime.datetime.now():
                update_connection_sock.send_json(intersection_connection_info)
                last_send_time = datetime.datetime.now()

    def set_send_socks(self, connections, output_queue, context):
        for send_conn in connections[output_queue]['streams']:
            if send_conn not in self.send_socks:
                send_sock = context.socket(zmq.PUSH)
                send_sock.connect(send_conn)
                self.send_socks[send_conn] = send_sock
                self.send_grouper = connections[output_queue]['grouping']

        deleted_connections = [connection for connection in self.send_socks.keys() if connection not in connections[output_queue]['streams']]
        for deleted_connection in deleted_connections:
            self.send_socks[deleted_connection].close()
            del self.send_socks[deleted_connection]

    def receive_messages(self, context=None, output_stream=None, grouper_cls=None):
        """
        Continously read and process using _process function

        """
        receive_sock = context.socket(zmq.PULL)
        self.receive_port = receive_sock.bind_to_random_port("tcp://*")
        set_timeouts_on_socket(receive_sock)

        while not self.controller_sock:
            time.sleep(1)
        if output_stream:
            while not self.controller_sock:
                time.sleep(1)

        while True:
            self._process(receive_sock, controller_sock=self.controller_sock)


