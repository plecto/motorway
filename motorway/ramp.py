import logging
import multiprocessing
from setproctitle import setproctitle
import datetime
from motorway.utils import set_timeouts_on_socket
import zmq
from time import time as _time


logger = logging.getLogger(__name__)


class Ramp(object):

    fields = []

    def __init__(self):
        super(Ramp, self).__init__()

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
    def run(cls, queue, controller_stream=None, result_stream=None):
        self = cls()
        context = zmq.Context()

        send_sock = context.socket(zmq.PUSH)
        send_sock.connect(queue)

        controller_sock = context.socket(zmq.PUSH)
        controller_sock.connect(controller_stream)
        set_timeouts_on_socket(controller_sock)

        receive_sock = context.socket(zmq.PULL)
        receive_sock.connect(result_stream)
        set_timeouts_on_socket(receive_sock)

        message_reply_poller = zmq.Poller()
        message_reply_poller.register(receive_sock, zmq.POLLIN)

        process_name = multiprocessing.current_process().name
        logger.info("Running %s" % process_name)
        logger.debug("%s pushing to %s" % (process_name, result_stream))
        setproctitle("data-pipeline: %s" % process_name, )
        while True:
            start_time = datetime.datetime.now()
            for received_message_result in self:
                for generated_message in received_message_result:
                    if generated_message is not None:
                        generated_message.send(send_sock)
                        if controller_sock:
                            generated_message.send_control_message(controller_sock, time_consumed=datetime.datetime.now() - start_time)
                    start_time = datetime.datetime.now()
                # After each batch send, let's see if we got replies
                message_replies = []
                for i in range(0, 100):
                    socks = dict(message_reply_poller.poll(timeout=0.01))
                    if socks.get(receive_sock) == zmq.POLLIN:
                        message_replies.append(receive_sock.recv_json())
                    else:
                        break
                for message_reply in message_replies:
                    if message_reply['status'] == 'success':
                        self.success(message_reply['id'])
                    elif message_reply['status'] == 'fail':
                        self.failed(message_reply['id'])
                    else:
                        logger.warn("Received unknown status feedback %s" % message_reply['status'])