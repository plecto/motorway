from Queue import Empty
import logging
import multiprocessing
import time
from time import time as _time
import uuid
import datetime
from motorway.messages import Message
from motorway.utils import set_timeouts_on_socket
import zmq
from setproctitle import setproctitle

logger = logging.getLogger(__name__)


class Intersection(object):

    fields = []

    def __init__(self):
        self.messages_processed = 0

    def _process(self, receive_sock, send_sock, controller_sock=None):

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
                    message = [Message.from_message(m, controller_sock) for m in value]
                else:
                    message = Message.from_message(value, controller_sock)
                try:
                    for generated_message in self.process(message):
                        if generated_message is not None and send_sock:
                            generated_message.send(send_sock)
                            if controller_sock:
                                generated_message.send_control_message(controller_sock)
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    if isinstance(message, list):
                        [m.fail() for m in message]
                    else:
                        message.fail()

        except Empty:  # Didn't receive anything from ZMQ
            pass

    def process(self, message):
        raise NotImplementedError()

    def ack(self, _id):
        """
        Lookup ramp and call ack on it!
        """
        pass

    @classmethod
    def run(cls, input_stream, output_stream=None, controller_stream=None):
        self = cls()
        process_name = multiprocessing.current_process().name
        logger.info("Running %s" % process_name)
        logger.debug("%s listening on %s and pushing to %s" % (process_name, input_stream, output_stream))
        setproctitle("data-pipeline: %s" % process_name)
        context = zmq.Context()

        receive_sock = context.socket(zmq.PULL)
        receive_sock.connect(input_stream)
        set_timeouts_on_socket(receive_sock)

        if output_stream:
            send_sock = context.socket(zmq.PUSH)
            send_sock.connect(output_stream)
        else:
            send_sock = None

        controller_sock = context.socket(zmq.PUSH)
        controller_sock.connect(controller_stream)
        set_timeouts_on_socket(controller_sock)

        while True:
            self._process(receive_sock, send_sock, controller_sock)


