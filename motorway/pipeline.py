from multiprocessing import Queue
from multiprocessing import Process
from setproctitle import setproctitle
import time
from motorway.controller import ControllerIntersection
from motorway.discovery.local import LocalDiscovery
from motorway.grouping import GroupingBuffer, HashRingGrouper
from motorway.utils import ramp_result_stream_name
from motorway.zmq_device import ZMQDevice
import zmq
import logging

logger = logging.getLogger(__name__)

class Pipeline(object):
    def __init__(self, discovery_backend=LocalDiscovery):
        self._streams = {}
        self._stream_consumers = {}
        self._processes = []
        self._queue_processes = []
        self._ramp_result_streams = []
        self._discovery_backend = discovery_backend()
        self.context = zmq.Context()

    def definition(self):
        raise NotImplementedError("You must implement a definition() on your pipeline")

    def _add_process(self, cls, process_instances, process_args, input_stream=None, output_stream=None, show_in_ui=True, process_start_number=0):
        if output_stream:
            if not output_stream in self._streams:
                self.spawn_zmq_device(output_stream)
                self._streams[output_stream] = True
        for i in range(process_start_number, process_instances + process_start_number):
            process_name = "%s-%s" % (cls.__name__, i)
            p = Process(
                target=cls.run,
                args=process_args,
                name=process_name
            )
            self._processes.append(p)
            if show_in_ui:
                if output_stream:
                    if output_stream not in self._stream_consumers:
                        self._stream_consumers[output_stream] = {'producers': [], 'consumers': []}
                    self._stream_consumers[output_stream]['producers'].append(process_name)
                if input_stream not in self._stream_consumers:
                    self._stream_consumers[input_stream] = {'producers': [], 'consumers': []}
                self._stream_consumers[input_stream]['consumers'].append(process_name)

    def add_ramp(self, ramp_class, output_stream, processes=1, controller_queue="_controller"):
        ramp_result_stream = ramp_result_stream_name(ramp_class.__name__)
        self._ramp_result_streams.append((ramp_class.__name__, ramp_result_stream))
        self._add_process(
            ramp_class,
            processes,
            process_args=(
                self._discovery_backend.get_queue_uri(output_stream, "push"),
                self._discovery_backend.get_queue_uri(controller_queue, 'push'),
                self._discovery_backend.get_queue_uri(ramp_result_stream, 'pull')
            ),
            output_stream=output_stream,
            input_stream=ramp_result_stream

        )

    def add_intersection(self, intersection_class, input_stream, output_stream=None, processes=1, grouper_cls=None):
        return self._add_intersection(intersection_class, input_stream, output_stream, processes, grouper_cls=grouper_cls)

    def _add_intersection(self, intersection_class, input_stream, output_stream=None, processes=1, controller_queue="_controller", grouper_cls=None):
        grouping_active = grouper_cls is not None and processes > 1
        grouper_queue_name = lambda process_number: "%s-grouper_for_%s" % (input_stream, process_number)

        if grouping_active:
            p = Process(
                target=GroupingBuffer.run,
                args=[
                    self._discovery_backend.get_queue_uri(input_stream, 'pull'),
                    lambda process_number: self._discovery_backend.get_queue_uri(grouper_queue_name(process_number), 'push'),
                    processes,
                    grouper_cls
                ],
                name="%s-%s" % (intersection_class.__name__, "buffer")
            )
            self._processes.append(p)

        for i in range(0, processes):
            if grouping_active:
                self.spawn_zmq_device(grouper_queue_name(i))
                intersection_input_stream = self._discovery_backend.get_queue_uri(grouper_queue_name(i), 'pull')
            else:
                intersection_input_stream = self._discovery_backend.get_queue_uri(input_stream, 'pull')
            if controller_queue:  # it's a regular intersection
                args = (
                    intersection_input_stream,
                    self._discovery_backend.get_queue_uri(output_stream, 'push'),
                    self._discovery_backend.get_queue_uri(controller_queue, 'push')
                )
            else:  # It's a controller
                ramp_result_streams = [
                    (
                        name,
                        self._discovery_backend.get_queue_uri(result_stream, 'push'))
                    for name, result_stream in self._ramp_result_streams
                ]
                args = (
                    intersection_input_stream,
                    self._discovery_backend.get_queue_uri(output_stream, 'push'),
                    self._stream_consumers,
                    ramp_result_streams
                )

            self._add_process(
                intersection_class,
                1,
                process_args=args,
                input_stream=input_stream,
                output_stream=output_stream,
                process_start_number=i
            )

    def spawn_zmq_device(self, queue):
        p = Process(target=ZMQDevice, args=(
            queue,
            self._discovery_backend.get_queue_uri(queue, 'push'),
            self._discovery_backend.get_queue_uri(queue, 'pull'),
        ), name="ZMQ Streamer %s" % queue)
        self._queue_processes.append(p)  # Make sure all queues are running (if possible) before everything else

    def _add_controller(self):
        controller_stream = "_controller"
        self._add_intersection(ControllerIntersection, controller_stream, controller_queue=False, grouper_cls=HashRingGrouper)

        self.spawn_zmq_device(controller_stream)  # TODO: Make public communication port

        for ramp_class_name, ramp_result_stream in self._ramp_result_streams:
            self.spawn_zmq_device(ramp_result_stream)

    def run(self, web_server=True, web_server_port=8700, service_port=9100):
        logger.info("Starting Pipeline!")

        setproctitle("data-pipeline: main")

        # User jobs
        self.definition()
        logger.debug("Loaded definition")

        # Controller Tranformer
        self._add_controller()

        logger.debug("Running queues")
        for queue_process in self._queue_processes:
            queue_process.start()
        logger.debug("Running pipeline")
        for process in self._processes:
            process.start()

        self._processes += self._queue_processes  # Monitor queue processes and shut then down with the rest
        try:
            while True:
                for process in self._processes:
                    assert process.is_alive()
                time.sleep(0.5)
        except Exception:
            raise
        finally:
            self.kill()
        logger.debug("Finished Pipeline!")

    def kill(self):
        for process in self._processes:
            logger.warn("Terminating %s" % process)
            if process.is_alive():
                process.terminate()
