from motorway.discovery.base import DiscoveryBase


class LocalDiscovery(DiscoveryBase):
    def get_queue_uri(self, queue, direction):
        assert direction in ('push', 'pull')
        return "ipc://livestats_motorway_%s-%s" % (queue, direction)