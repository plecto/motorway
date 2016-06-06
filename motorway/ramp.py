import logging
import multiprocessing
from setproctitle import setproctitle
import datetime
from threading import Thread
import uuid
from motorway.grouping import GroupingValueMissing
from motorway.mixins import GrouperMixin, SendMessageMixin
from motorway.utils import set_timeouts_on_socket, get_connections_block
import zmq
import time
import random


logger = logging.getLogger(__name__)


class Ramp(GrouperMixin, SendMessageMixin, object):

    def __init__(self, runs_on_controller=False):
        super(Ramp, self).__init__()
        self.runs_on_controller = runs_on_controller
        self.send_socks = {}
        self.send_grouper = None
        self.result_port = None

    def next(self):
        """
        This function is called continuously by the ramp.

        .. WARNING::
            Do not block this for a long time or make a while True loop inside it. Betwen every :func:`motorway.ramp.Ramp.next` run,
            some essential operations are run, including receiving acks from the :class:`motorway.controller.Controller`

        :yield: :class:`motorway.messages.Message` instance
        """
        raise NotImplementedError()

    def __iter__(self):
        yield self.next()

    def success(self, _id):
        """
        Called when a message was successfully ack'ed through the entire pipeline.

        :param _id: The id of the message that was successful
        """
        pass

    def failed(self, _id):
        """
        Called when a message failed somewhere in the pipeline. The message might not be entirely finished processing
        at this point and this function might be called multiple times.

        :param _id: The id of the message that failed
        """
        pass

    @classmethod
    def run(cls, queue, refresh_connection_stream=None, runs_on_controller=False):

        self = cls(runs_on_controller=runs_on_controller)

        process_name = multiprocessing.current_process().name
        process_uuid = str(uuid.uuid4())
        process_id = "_ramp-%s" % process_uuid
        logger.info("Running %s" % process_name)
        logger.debug("%s pushing to %s" % (process_name, queue))
        setproctitle("data-pipeline: %s" % process_name, )

        context = zmq.Context()

        # Create Thread Factories :-)

        thread_update_connections_factory = lambda: Thread(target=self.connection_thread, name="connection_thread", kwargs={
            'refresh_connection_stream': refresh_connection_stream,
            'context': context,
            'queue': queue,
            'process_id': process_id,
            'process_name': process_name,
        })

        thread_main_factory = lambda: Thread(target=self.message_producer, name="message_producer", kwargs={
            'context': context,
            'process_id': process_id,
        })

        thread_results_factory = lambda: Thread(target=self.receive_replies, name="results", kwargs={
            'context': context,
        })

        # Run threads

        thread_update_connections = thread_update_connections_factory()
        thread_update_connections.start()

        thread_main = thread_main_factory()
        thread_main.start()

        thread_results = thread_results_factory()
        thread_results.start()

        while True:
            if not thread_update_connections.isAlive():
                logger.error("Thread thread_update_connections crashed in %s" % self.__class__.__name__)
                thread_update_connections = thread_update_connections_factory()
                thread_update_connections.start()
            if not thread_main.isAlive():
                logger.error("Thread thread_main crashed in %s" % self.__class__.__name__)
                thread_main = thread_main_factory()
                thread_main.start()
            if not thread_results.isAlive():
                logger.error("Thread thread_results crashed in %s" % self.__class__.__name__)
                thread_results = thread_results_factory()
                thread_results.start()
            time.sleep(10)

    def connection_thread(self, context=None, refresh_connection_stream=None, queue=None, process_id=None,
                          process_name=None):
        refresh_connection_sock = context.socket(zmq.SUB)
        refresh_connection_sock.connect(refresh_connection_stream)
        refresh_connection_sock.setsockopt(zmq.SUBSCRIBE, '')  # You must subscribe to something, so this means *all*
        set_timeouts_on_socket(refresh_connection_sock)

        # Wait for a result port and register as consumer of input stream
        while self.result_port is None:
            logger.debug("Waiting for result port")
            time.sleep(1)
        connections = get_connections_block('_update_connections', refresh_connection_sock)
        update_connection_sock = context.socket(zmq.PUSH)
        update_connection_sock.connect(connections['_update_connections']['streams'][0])
        ramp_connection_info = {
            'streams': {
                process_id: ['tcp://127.0.0.1:%s' % self.result_port]
            },
            'meta': {
                'id': process_id,
                'name': process_name,
                'grouping': None
            }
        }
        update_connection_sock.send_json(ramp_connection_info)
        last_send_time = datetime.datetime.now()

        connections = get_connections_block('_message_ack', refresh_connection_sock)
        self.controller_sock = context.socket(zmq.PUSH)
        self.controller_sock.connect(connections['_message_ack']['streams'][0])
        set_timeouts_on_socket(self.controller_sock)


        while True:
            try:
                connections = refresh_connection_sock.recv_json()
                if queue in connections:
                    for send_conn in connections[queue]['streams']:
                        if send_conn not in self.send_socks:
                            send_sock = context.socket(zmq.PUSH)
                            send_sock.connect(send_conn)
                            self.send_socks[send_conn] = send_sock
                            self.send_grouper = connections[queue]['grouping']
                    deleted_connections = [connection for connection in self.send_socks.keys() if connection not in connections[queue]['streams']]
                    for deleted_connection in deleted_connections:
                        self.send_socks[deleted_connection].close()
                        del self.send_socks[deleted_connection]
            except zmq.Again:
                time.sleep(1)
            if last_send_time + datetime.timedelta(seconds=10) < datetime.datetime.now():
                update_connection_sock.send_json(ramp_connection_info)
                last_send_time = datetime.datetime.now()

    def receive_replies(self, context=None):
        result_sock = context.socket(zmq.PULL)
        self.result_port = result_sock.bind_to_random_port("tcp://*")
        logger.debug("Result port is %s" % self.result_port)
        set_timeouts_on_socket(result_sock)

        while True:
            try:
                message_reply = result_sock.recv_json()
                if message_reply['status'] == 'success':
                    self.success(message_reply['id'])
                elif message_reply['status'] == 'fail':
                    self.failed(message_reply['id'])
                else:
                    logger.warn("Received unknown status feedback %s" % message_reply['status'])
            except zmq.Again:
                time.sleep(1)

    def should_run(self):
        """
        Subclass to define rules whether this tap should run or not. Mainly used for ensuring a tap only runs once
         across the network

        :return: bool
        """
        return self.runs_on_controller

    def message_producer(self, context=None, process_id=None):

        while True:
            if not self.send_socks:
                logger.debug("Waiting for send_socks")
                time.sleep(1)
            elif self.should_run():
                start_time = datetime.datetime.now()
                try:
                    for received_message_result in self:
                        for generated_message in received_message_result:
                            if generated_message is not None:
                                self.send_message(generated_message, process_id, time_consumed=datetime.datetime.now() - start_time, sender='ramp')
                            start_time = datetime.datetime.now()
                except Exception as e:  # Never let user code crash the whole thing!!
                    logger.exception(e)
