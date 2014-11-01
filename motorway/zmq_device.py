from setproctitle import setproctitle
from signal import signal, SIGTERM
import zmq
import logging

logger = logging.getLogger(__name__)


class ZMQDevice(object):
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

            # Socket facing services
            backend = context.socket(zmq.PUSH)
            backend.bind(pull_stream)  # Notice the name is inverted from socket

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