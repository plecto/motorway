import logging
import multiprocessing
from setproctitle import setproctitle
import datetime
from threading import Thread
import uuid
from motorway.grouping import GroupingValueMissing
from motorway.messages import Message
from motorway.mixins import GrouperMixin, SendMessageMixin, ConnectionMixin
from motorway.utils import set_timeouts_on_socket, get_connections_block
import zmq
import time
import random


logger = logging.getLogger(__name__)


class Ramp(GrouperMixin, SendMessageMixin, ConnectionMixin, object):
    """
    All messages must at some point start at a ramp, which ingests data into the pipeline from an external system or
    generates data it self (such as random words in the tutorial)
    """

    send_control_messages = True
    sleep_time = 0

    def __init__(self, runs_on_controller=False, process_uuid=None):
        super(Ramp, self).__init__()
        self.runs_on_controller = runs_on_controller
        self.send_socks = {}
        self.send_grouper = None
        self.receive_port = None
        self.process_name = multiprocessing.current_process().name
        self.process_uuid = "_ramp-%s" % process_uuid
        self.grouper_instance = None

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
    def run(cls, queue, refresh_connection_stream=None, runs_on_controller=False, process_uuid=None):

        self = cls(runs_on_controller=runs_on_controller, process_uuid=process_uuid)

        logger.info("Running %s" % self.process_name)
        logger.debug("%s pushing to %s" % (self.process_name, queue))
        setproctitle("data-pipeline: %s" % self.process_name, )

        context = zmq.Context()

        # Create Thread Factories :-)

        thread_update_connections_factory = lambda: Thread(target=self.connection_thread, name="connection_thread", kwargs={
            'refresh_connection_stream': refresh_connection_stream,
            'context': context,
            'input_queue': self.process_uuid,
            'output_queue': queue,
        })
        #self, context=None, refresh_connection_stream=None, input_queue=None, output_queue=None, process_id=None,
                          #process_name=None, grouper_cls=None, set_controller_sock=True

        thread_main_factory = lambda: Thread(target=self.message_producer, name="message_producer", kwargs={
            'context': context,
            'process_id': self.process_uuid,
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

    def receive_replies(self, context=None):
        result_sock = context.socket(zmq.PULL)
        self.receive_port = result_sock.bind_to_random_port("tcp://*")
        logger.debug("Result port is %s" % self.receive_port)
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
                if self.sleep_time:
                    time.sleep(self.sleep_time)
