import random
from hash_ring import HashRing
import zmq
import logging

logger = logging.getLogger(__name__)


class GroupingValueMissing(Exception):
    pass


class BaseGrouper(object):
    def __init__(self, destinations):
        self.destinations = destinations

    def get_destinations_for(self, value):
        raise NotImplementedError("Please implement get_destinations_for")


class HashRingGrouper(BaseGrouper):
    """
    Group messages based on a consistent hashing algorithm. e.g. a message with the same grouping key will always
    go to the same intersection
    """
    def __init__(self, *args, **kwargs):
        super(HashRingGrouper, self).__init__(*args, **kwargs)
        self.hash_ring = HashRing(self.destinations)

    def get_destinations_for(self, value):
        if type(value) is int:
            value = str(value)
        try:
            return [self.hash_ring.get_node(value)]
        except TypeError:
            raise GroupingValueMissing("'%s' is an invalid grouping value for HashRingGrouper" % (value, ))


class RandomGrouper(BaseGrouper):
    """
    Select a random intersection to receive the message
    """
    def get_destinations_for(self, value):
        return [random.choice(self.destinations)]


class SendToAllGrouper(BaseGrouper):
    """
    Send messages to all intersections
    """
    def get_destinations_for(self, value):
        return self.destinations
