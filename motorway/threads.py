import logging
import multiprocessing
from setproctitle import setproctitle
import time
import zmq

logger = logging.getLogger(__name__)


class ThreadRunner(object):

    def __init__(self):
        super(ThreadRunner, self).__init__()

    def thread_factory(self, *args, **kwargs):
        raise NotImplementedError("Define the threads your want to the ThreadRunner to manage in self.thread_factory()")

    @classmethod
    def run(cls, *args, **kwargs):
        self = cls(process_uuid=kwargs.get('process_uuid'))
        process_name = multiprocessing.current_process().name
        setproctitle("data-pipeline: %s" % process_name)

        factories = {factory: None for factory in self.thread_factory(*args, **kwargs)}

        logger.info("Running %s" % process_name)
        # Run all thread factories
        for factory in factories.keys():
            factories[factory] = factory()
            factories[factory].start()

        # Monitor and restart factories if needed
        while True:
            for factory, thread in factories.items():
                if not thread.isAlive():
                    logger.warn("Thread %s crashed in %s. Restarting" % (factory(), self.__class__.__name__))
                    factories[factory] = factory()
                    factories[factory].start()
            time.sleep(10)