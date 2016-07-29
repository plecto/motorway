import calendar
import logging
import datetime
import socket

from motorway.intersection import Intersection
from motorway.messages import Message
from motorway.utils import set_timeouts_on_socket, get_ip
import time
import zmq

logger = logging.getLogger(__name__)

current_heartbeat = lambda: calendar.timegm(datetime.datetime.utcnow().timetuple())


class ConnectionIntersection(Intersection):
    """
    Responsible to receiving and publishing information about the different message endpoints. Every ramp/intersection
    will subscribe to this information and use it for routing the messages correctly.

    While implemented with the intersection interface, it is currently only possibly to run this as a single instance
    as it has internal state which is not shared.

    It should be possible to extend this class and override it to store state in external systems such as Consul,
    Zookeper or something else.
    """

    HEARTBEAT_TIMEOUT = 30

    send_control_messages = False

    def __init__(self, bind_address='tcp://0.0.0.0:7007', **kwargs):
        super(ConnectionIntersection, self).__init__(**kwargs)
        self.process_statistics = {}
        self.ramp_socks = {}
        self.process_address_to_uuid = {}
        self.queue_processes = {}
        self.process_id_to_name = {}
        self.bind_address = bind_address

    def process(self, message):
        connection_updates = message.content
        self.process_id_to_name[connection_updates['meta']['id']] = connection_updates['meta']['name']
        for queue, consumers in connection_updates['streams'].items():
            if queue not in self.queue_processes:
                self.queue_processes[queue] = {
                    'streams': [],
                    'stream_heartbeats': {},
                    'grouping': None
                }
            for consumer in consumers:
                if consumer not in self.queue_processes[queue]['streams']:
                    # self.process_statistics[connection_updates['meta']['id']]['status'] = 'running'
                    self.process_address_to_uuid[consumer] = connection_updates['meta']['id']
                    # Add to streams and update grouping TODO: Keep track of multiple groupings
                    self.queue_processes[queue]['streams'].append(consumer)
                    self.queue_processes[queue]['grouping'] = connection_updates['meta']['grouping']
                self.queue_processes[queue]['stream_heartbeats'][consumer] = {
                    'heartbeat': current_heartbeat(),
                    'process_name': connection_updates['meta']['name'],
                    'process_id': connection_updates['meta']['id']
                }
                logger.debug("Received heartbeat from %s on queue %s - current value %s" % (consumer, queue, self.queue_processes[queue]['stream_heartbeats'][consumer]))
        yield

    def connection_thread(self, context=None, **kwargs):
        while not self.receive_port:
            time.sleep(1)

        # The publish/broadcast socket where clients subscribe to updates
        broadcast_connection_sock = context.socket(zmq.PUB)
        broadcast_connection_sock.bind(self.bind_address)
        set_timeouts_on_socket(broadcast_connection_sock)

        self.queue_processes['_update_connections'] = {
            'streams': ['tcp://%s:%s' % (get_ip(), self.receive_port)],
            'grouping': None,
            'stream_heartbeats': {}
        }
        while True:
            for queue, consumers in self.queue_processes.items():
                for consumer, heartbeat_info in consumers['stream_heartbeats'].items():
                    if current_heartbeat() > (heartbeat_info['heartbeat'] + self.HEARTBEAT_TIMEOUT):
                        logger.warn("Removing %s from %s due to missing heartbeat" % (consumer, queue))
                        self.queue_processes[queue]['streams'].remove(consumer)
                        self.queue_processes[queue]['stream_heartbeats'].pop(consumer)
                        # self.process_statistics[heartbeat_info['process_id']]['status'] = 'failed'

            # Send the current connections
            broadcast_connection_sock.send_json(self.queue_processes)
            logger.debug("Announced %s", self.queue_processes)

            # This intersection should it self publish to send socks..
            # self.set_send_socks(self.queue_processes, self.output_queue, self.context)
            time.sleep(5)