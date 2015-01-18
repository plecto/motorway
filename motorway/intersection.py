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
from motorway.utils import set_timeouts_on_socket, get_connections_block
import zmq
from setproctitle import setproctitle

logger = logging.getLogger(__name__)


class Intersection(object):

    fields = []

    def __init__(self):
        self.messages_processed = 0
        self.process_uuid = str(uuid.uuid4())
        self.receive_port = None
        self.send_socks = {}
        self.controller_sock = None

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
                    else:
                        time.sleep(1)
                self.messages_processed += len(value)
            else:
                poller = zmq.Poller()
                poller.register(receive_sock, zmq.POLLIN)
                socks = dict(poller.poll(timeout=1000))
                if socks.get(receive_sock) == zmq.POLLIN:
                    value = receive_sock.recv_json()
                else:
                    return
                self.messages_processed += 1
            if value:
                if isinstance(value, list):
                    message = [Message.from_message(m, controller_sock, process_name=self.process_uuid) for m in value]
                else:
                    message = Message.from_message(value, controller_sock, process_name=self.process_uuid)
                try:
                    for generated_message in self.process(message):
                        if generated_message is not None and self.send_socks:
                            generated_message.send(random.choice(self.send_socks.values()), self.process_uuid)
                            if controller_sock:
                                generated_message.send_control_message(controller_sock, process_name=self.process_uuid)
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    if isinstance(message, list):
                        [m.fail() for m in message]
                    else:
                        message.fail()

        except Empty:  # Didn't receive anything from ZMQ
            pass

    def process(self, message):
        """
        This function is called continuously by the intersection.

        :yield: :class:`motorway.messages.Message` instance
        :param message: :class:`motorway.messages.Message` instance or :func:`list` if using
            :func:`motorway.decorators.batch_process`

        """
        raise NotImplementedError()

    @classmethod
    def run(cls, input_stream, output_stream=None, refresh_connection_stream=None):
        self = cls()
        process_name = multiprocessing.current_process().name
        logger.info("Running %s" % process_name)
        logger.debug("%s listening on %s and pushing to %s" % (process_name, input_stream, output_stream))
        setproctitle("data-pipeline: %s" % process_name)
        context = zmq.Context()

        # Run threads

        thread_update_connections = Thread(target=self.connection_thread, name="connection_thread", kwargs={
            'refresh_connection_stream': refresh_connection_stream,
            'context': context,
            'input_queue': input_stream,
            'output_queue': output_stream,
            'process_id': self.process_uuid,
            'process_name': process_name,
        })
        thread_update_connections.start()

        thread_main = Thread(target=self.receive_messages, name="message_producer", kwargs={
            'context': context,
            # 'process_id': self.process_uuid,
            'output_stream': output_stream,
        })
        thread_main.start()

        while True:
            time.sleep(1)

    def connection_thread(self, context=None, refresh_connection_stream=None, input_queue=None, output_queue=None, process_id=None,
                          process_name=None):
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
        update_connection_sock.send_json({
            'streams': {
                input_queue: ['tcp://127.0.0.1:%s' % self.receive_port]
            },
            'meta': {
                'id': process_id,
                'name': process_name,
                'grouping': None
            }
        })

        connections = get_connections_block('_message_ack', refresh_connection_sock, existing_connections=connections)
        self.controller_sock = context.socket(zmq.PUSH)
        self.controller_sock.connect(connections['_message_ack']['streams'][0])
        set_timeouts_on_socket(self.controller_sock)

        while True:
            try:
                connections = refresh_connection_sock.recv_json()
                if output_queue in connections:
                    for send_conn in connections[output_queue]['streams']:
                        if send_conn not in self.send_socks:
                            send_sock = context.socket(zmq.PUSH)
                            send_sock.connect(send_conn)
                            self.send_socks[send_conn] = send_sock
            except zmq.Again:
                pass

    def receive_messages(self, context=None, output_stream=None):
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


