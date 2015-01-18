import logging
import multiprocessing
from setproctitle import setproctitle
import datetime
import uuid
from motorway.utils import set_timeouts_on_socket, get_connections_block
import zmq
import time


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
    def run(cls, queue, refresh_connection_stream=None):

        process_name = multiprocessing.current_process().name
        process_uuid = str(uuid.uuid4())
        process_id = "_ramp-%s" % process_uuid
        logger.info("Running %s" % process_name)
        logger.debug("%s pushing to %s" % (process_name, queue))
        setproctitle("data-pipeline: %s" % process_name, )

        self = cls()
        context = zmq.Context()

        refresh_connection_sock = context.socket(zmq.SUB)
        refresh_connection_sock.connect(refresh_connection_stream)
        refresh_connection_sock.setsockopt(zmq.SUBSCRIBE, '')  # You must subscribe to something, so this means *all*
        set_timeouts_on_socket(refresh_connection_sock)

        result_sock = context.socket(zmq.PULL)
        result_port = result_sock.bind_to_random_port("tcp://*")
        set_timeouts_on_socket(result_sock)

        message_reply_poller = zmq.Poller()
        message_reply_poller.register(result_sock, zmq.POLLIN)

        # Register as consumer of input stream
        connections = get_connections_block('_update_connections', refresh_connection_sock)
        update_connection_sock = context.socket(zmq.PUSH)
        update_connection_sock.connect(connections['_update_connections'][0])
        update_connection_sock.send_json({
            'streams': {
                process_id: ['tcp://127.0.0.1:%s' % result_port]
            },
            'meta': {
                'id': process_id,
                'name': process_name
            }
        })

        connections = get_connections_block(queue, refresh_connection_sock, existing_connections=connections)
        send_sock = context.socket(zmq.PUSH)
        send_sock.connect(connections[queue][0])  # TODO: Multiple receivers!

        connections = get_connections_block('_message_ack', refresh_connection_sock, existing_connections=connections)
        controller_sock = context.socket(zmq.PUSH)
        controller_sock.connect(connections['_message_ack'][0])
        set_timeouts_on_socket(controller_sock)

        while True:
            start_time = datetime.datetime.now()
            for received_message_result in self:
                for generated_message in received_message_result:
                    if generated_message is not None:
                        generated_message.send(send_sock)
                        if controller_sock:
                            generated_message.send_control_message(controller_sock, time_consumed=datetime.datetime.now() - start_time, process_name=process_id)
                    start_time = datetime.datetime.now()
                # After each batch send, let's see if we got replies
                message_replies = []
                for i in range(0, 100):
                    socks = dict(message_reply_poller.poll(timeout=0.01))
                    if socks.get(result_sock) == zmq.POLLIN:
                        message_replies.append(result_sock.recv_json())
                    else:
                        break
                for message_reply in message_replies:
                    if message_reply['status'] == 'success':
                        self.success(message_reply['id'])
                    elif message_reply['status'] == 'fail':
                        self.failed(message_reply['id'])
                    else:
                        logger.warn("Received unknown status feedback %s" % message_reply['status'])