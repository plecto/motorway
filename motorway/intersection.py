import json
import os
import random
from queue import Empty
import logging
import multiprocessing
from threading import Thread
import time
from time import time as _time
import uuid
import datetime

from motorway.exceptions import SocketBlockedException
from motorway.instrumentation import instrumentation_manager
from motorway.messages import Message
from motorway.mixins import GrouperMixin, SendMessageMixin, ConnectionMixin
from motorway.threads import ThreadRunner
from motorway.utils import set_timeouts_on_socket, message_processing_manager
import zmq

logger = logging.getLogger(__name__)


class Intersection(GrouperMixin, SendMessageMixin, ConnectionMixin, ThreadRunner):
    """
    Intersections receive messages and generate either:

    - A spin-off message

    Spin-off messages will keep track of the state of the entire message tree and re-run it if failed. This means that
    if you want to re-run the message all the way from the ramp in case of an error, you should make a spin-off message.

    Message.new(message, {
        {
            'word': 'hello',
            'count': 1
        },
        grouping_value='hello'
    })

    - A brand new message

    The message will be created with the intersection as producer. The intersection will not receive feedback if it
    is successful or not and hence will not be re-tried in the case of an error.

    Message(uuid.uuid4()
    """

    send_control_messages = True

    def __init__(self, process_uuid=None):
        super(Intersection, self).__init__()
        self.messages_processed = 0
        self.process_uuid = process_uuid.hex if process_uuid else uuid.uuid4().hex
        self.process_name = multiprocessing.current_process().name
        self.receive_port = None
        self.report_address = None
        self.send_socks = {}
        self.send_grouper = None
        self.controller_sock = None
        self.message_batch_start = datetime.datetime.now()  # This is used to time how much time messages take
        self.message_being_processed = None
        self.process_id_to_name = {}  # Maps process UUIDs to human readable names
        self.process_address_to_uuid = {}  # Maps TCP endpoints to human readable names
        self.process_id_to_report_address = {}  # Maps process UUIDs to TCP endpoints for reporting message processing
        self.grouper_instance = None

    def thread_factory(self, input_stream, output_stream=None, refresh_connection_stream=None, grouper_cls=None, process_uuid=None):
        context = zmq.Context()
        thread_factories_to_run = []

        # Create Thread Factories :-)

        thread_update_connections_factory = lambda: Thread(target=self.connection_thread, name="connection_thread", kwargs={
            'refresh_connection_stream': refresh_connection_stream,
            'context': context,
            'input_queue': input_stream,
            'output_queue': output_stream,
            'grouper_cls': grouper_cls
        })
        thread_factories_to_run.append(thread_update_connections_factory)

        thread_main_factory = lambda: Thread(target=self.receive_messages, name="message_producer", kwargs={
            'context': context,
            'output_stream': output_stream,
            'grouper_cls': grouper_cls,
        })
        thread_factories_to_run.append(thread_main_factory)

        if self.send_control_messages:
            thread_report_message_being_processed_factory = lambda: Thread(
                target=self.report_message_being_processed,
                name="report_message_being_processed",
                kwargs={'context': context}
            )
            thread_factories_to_run.append(thread_report_message_being_processed_factory)

        return thread_factories_to_run

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

                    with message_processing_manager(self, message):
                        for generated_message in self.process_instrumented(message):
                            if generated_message is not None and self.send_socks:
                                self.send_message(generated_message, self.process_uuid, time_consumed=(datetime.datetime.now() - self.message_batch_start), control_message=self.send_control_messages)
                                self.message_batch_start = datetime.datetime.now()
                except Exception as e:
                    logger.error(str(e), exc_info=True)

                    # Don't send control messages if e.g. web server or other system process
                    if self.send_control_messages:
                        if isinstance(message, list):
                            [m.fail() for m in message]
                        else:
                            message.fail()

        except Empty:  # Didn't receive anything from ZMQ
            pass

    def process_instrumented(self, message):
        with instrumentation_manager("%s.process" % self.__class__.__name__):
            for generated_message in self.process(message):
                yield generated_message

    def ack(self, message, retries=100):
        for index in range(0, retries):
            try:
                message.ack(time_consumed=(datetime.datetime.now() - self.message_batch_start))
                break
            except zmq.Again:
                time.sleep(random.randrange(1, 3))  # to avoid peak loads when running multiple processes
        else:  # if for loop exited cleanly (no break)
            raise SocketBlockedException("Acknowledge for process %s could not be sent after %s attempts" % (self.process_name, retries))

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

    def receive_messages(self, context=None, output_stream=None, grouper_cls=None):
        """
        Continously read and process using _process function

        """
        receive_sock = context.socket(zmq.PULL)
        # Allow overwriting buffer sizes to increase/decrease size of the process queues
        # Default is equal to around 12.000 queued messages per process (depending on the OS-config)
        receive_sock.SNDBUF = os.environ.get('MOTORWAY_SOCKET_SNDBUF', -1)
        receive_sock.RCVBUF = os.environ.get('MOTORWAY_SOCKET_RCVBUF', -1)
        self.receive_port = receive_sock.bind_to_random_port("tcp://*")
        set_timeouts_on_socket(receive_sock)

        if self.send_control_messages:
            self._wait_for_controller_sock()

        while True:
            self._process(receive_sock, controller_sock=self.controller_sock)

    def report_message_being_processed(self, context=None):
        socket = context.socket(zmq.REP)
        self.report_address = socket.bind_to_random_port('tcp://*')

        while True:
            _ = socket.recv()  # wait for request from WebserverIntersection

            message_being_processed = self.message_being_processed
            if message_being_processed:
                msgs_being_processed = self._format_message_being_processed(message_being_processed)
            else:
                msgs_being_processed = []

            socket.send_json(json.dumps(msgs_being_processed))

    @staticmethod
    def _format_message_being_processed(message_being_processed):
        msgs_formatted = []
        if isinstance(message_being_processed, list):
            for msg in message_being_processed:
                msgs_formatted.append({
                    'ramp_unique_id': msg.ramp_unique_id,
                    'content': msg.content,
                    'init_time': msg.init_time.isoformat(),
                })
        else:
            msgs_formatted.append({
                'ramp_unique_id': message_being_processed.ramp_unique_id,
                'content': message_being_processed.content,
                'init_time': message_being_processed.init_time.isoformat(),
            })

        return msgs_formatted

    def _wait_for_controller_sock(self):
        while not self.controller_sock:
            time.sleep(1)
