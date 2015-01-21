

class DiscoveryBase(object):
    def get_queue_uri(self, queue, direction):
        assert direction in ('push', 'pull')