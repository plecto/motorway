import random
from hash_ring import HashRing
import zmq
import logging

logger = logging.getLogger(__name__)


class GroupingValueMissing(Exception):
    pass


class HashRingGrouper(object):
    def __init__(self, destinations):
        self.hash_ring = HashRing(destinations)

    def get_destination_for(self, value):
        if type(value) is int:
            value = str(value)
        try:
            return self.hash_ring.get_node(value)
        except TypeError:
            raise GroupingValueMissing("'%s' is an invalid grouping value for HashRingGrouper" % (value, ))


class RandomGrouper(object):
    def __init__(self, destinations):
        self.destinations = destinations

    def get_destination_for(self, value):
        return random.choice(self.destinations)


class GroupingBuffer(object):
    @classmethod
    def run(cls, input_stream, output_stream_function, number_of_outputs, grouper_cls):
        logger.info("Running grouper %s" % input_stream)
        context = zmq.Context()

        receive_sock = context.socket(zmq.PULL)
        receive_sock.connect(input_stream)

        grouper = grouper_cls(number_of_outputs)

        processes = {}

        for process in range(0, number_of_outputs):
            send_sock = context.socket(zmq.PUSH)
            logger.debug("Grouper listening to %s and pushing to %s" % (input_stream, output_stream_function(process)))
            send_sock.connect(output_stream_function(process))
            processes[process] = send_sock

        while True:
            poller = zmq.Poller()
            poller.register(receive_sock, zmq.POLLIN)
            socks = dict(poller.poll(timeout=1000))
            if socks.get(receive_sock) == zmq.POLLIN:
                value = receive_sock.recv_json()
                destination = grouper.get_destination_for(str(value['grouping_value']))
                processes[destination].send_json(value)