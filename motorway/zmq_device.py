from setproctitle import setproctitle
from signal import signal, SIGTERM
import zmq
import logging

logger = logging.getLogger(__name__)


class ZMQDevice(object):
    """
    This is a fake "Queue" which allows multiple producers to push to it and multiple consumers to consume from it
    """
    def __init__(self, queue, push_stream, pull_stream):
        setproctitle("data-pipeline: %s streamer" % queue)
        logger.debug("ZMQDevice %s %s %s" % (queue, push_stream, pull_stream))

        def close_sockets(signum, stack_frame):
            frontend.close()
            backend.close()
            context.term()
        signal(SIGTERM, close_sockets)

        frontend = None
        backend = None
        context = None

        try:
            context = zmq.Context(1)
            # Socket facing clients
            frontend = context.socket(zmq.PULL)
            frontend.bind(push_stream)  # Notice the name is inverted from socket
            frontend.setsockopt(zmq.RCVHWM, 10000)  # Allow this "Queue" to contain up to 10k items
            frontend.setsockopt(zmq.SNDHWM, 10000)  # Allow this "Queue" to contain up to 10k items

            # Socket facing services
            backend = context.socket(zmq.PUSH)
            backend.bind(pull_stream)  # Notice the name is inverted from socket
            backend.setsockopt(zmq.RCVHWM, 10000)  # Allow this "Queue" to contain up to 10k items
            backend.setsockopt(zmq.SNDHWM, 10000)  # Allow this "Queue" to contain up to 10k items

            zmq.device(zmq.STREAMER, frontend, backend)
        except Exception, e:
            logger.exception(e)
        finally:
            if frontend:
                frontend.close()
            if backend:
                backend.close()
            if context:
                context.term()
        exit(0)