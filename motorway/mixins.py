import datetime

import itertools
import random
import socket
import uuid

import zmq
import time

from motorway.exceptions import SocketBlockedException
from motorway.grouping import HashRingGrouper, RandomGrouper, GroupingValueMissing, SendToAllGrouper
from motorway.messages import Message
from motorway.utils import set_timeouts_on_socket, get_connections_block, get_ip


class GrouperMixin(object):
    def get_grouper(self, grouper_name):
        if grouper_name == "HashRingGrouper":
            return HashRingGrouper
        elif grouper_name == "SendToAllGrouper":
            return SendToAllGrouper
        else:
            return RandomGrouper


class SendMessageMixin(object):

    def send_message(self, message, process_id, time_consumed=None, sender=None, control_message=True, retries=100):
        """

        :param message: A ::pipeline.messages.Message:: instance
        :param process_id:
        :param time_consumed:
        :param sender:
        :param control_message:
        :param retries:
        :type retries: int
        :raises: SocketBlockedException, GroupingValueMissing
        :return:
        """
        try:
            socket_addresses = self.grouper_instance.get_destinations_for(message.grouping_value)
        except GroupingValueMissing:
            raise GroupingValueMissing("Message '%s' provided an invalid grouping_value: '%s'" % (message.content, message.grouping_value))
        for destination in socket_addresses:
            message.send(
                self.send_socks[destination],
                process_id
            )
            if self.controller_sock and self.send_control_messages and control_message:
                for index in xrange(0, retries):
                    try:
                        message.send_control_message(
                            self.controller_sock,
                            time_consumed=time_consumed,
                            process_name=process_id,
                            destination_uuid=self.process_address_to_uuid[destination],
                            destination_endpoint=destination,
                            sender=sender
                        )
                        break
                    except KeyError:  # If destination is not known, lookup fails
                        time.sleep(random.randrange(1, 3))  # random to avoid peak loads when running multiple processes
                    except zmq.Again:  # If connection fails, retry
                        time.sleep(random.randrange(1, 3))
                else:  # if for loop exited cleanly (no break)
                    raise SocketBlockedException("Control Message for process %s could not be sent after %s attempts"
                                                 % (process_id, retries))


class ConnectionMixin(object):
    def set_send_socks(self, connections, output_queue, context):
        for send_conn in connections[output_queue]['streams']:
            if send_conn not in self.send_socks:
                send_sock = context.socket(zmq.PUSH)
                send_sock.connect(send_conn)
                self.send_socks[send_conn] = send_sock
                self.send_grouper = connections[output_queue]['grouping']

        deleted_connections = [connection for connection in self.send_socks.keys() if connection not in connections[output_queue]['streams']]
        for deleted_connection in deleted_connections:
            self.send_socks[deleted_connection].close()
            del self.send_socks[deleted_connection]

        # initialize grouper again with new socks
        self.grouper_instance = self.get_grouper(self.send_grouper)(
            self.send_socks.keys()
        )

    def connection_thread(self, context=None, refresh_connection_stream=None, input_queue=None, output_queue=None,
                          grouper_cls=None, set_controller_sock=True):
        refresh_connection_sock = context.socket(zmq.SUB)
        refresh_connection_sock.connect(refresh_connection_stream)
        refresh_connection_sock.setsockopt(zmq.SUBSCRIBE, '')  # You must subscribe to something, so this means *all*}
        set_timeouts_on_socket(refresh_connection_sock)

        connections = get_connections_block('_update_connections', refresh_connection_sock)

        while not self.receive_port:
            time.sleep(1)

        # Register as consumer of input stream
        update_connection_sock = context.socket(zmq.PUSH)
        update_connection_sock.connect(connections['_update_connections']['streams'][0])
        intersection_connection_info = {
            'streams': {
                input_queue: ['tcp://%s:%s' % (get_ip(), self.receive_port)]
            },
            'meta': {
                'id': self.process_uuid,
                'name': self.process_name,
                'grouping': None if not grouper_cls else grouper_cls.__name__  # Specify how messages sent to this intersection should be grouped
            }
        }
        Message(None, intersection_connection_info).send(update_connection_sock, producer_uuid=self.process_uuid)
        last_send_time = datetime.datetime.now()

        if set_controller_sock:
            connections = get_connections_block('_message_ack', refresh_connection_sock, existing_connections=connections)
            self.controller_sock = context.socket(zmq.PUSH)
            self.controller_sock.connect(connections['_message_ack']['streams'][0])
            set_timeouts_on_socket(self.controller_sock)
        while True:
            try:
                connections = refresh_connection_sock.recv_json()
                if output_queue in connections:
                    self.set_send_socks(connections, output_queue, context)
                self.process_id_to_name = {process['process_id']: process['process_name'] for process in itertools.chain.from_iterable([queue_details['stream_heartbeats'].values() for queue_details in connections.values()])}
                self.process_address_to_uuid = {address: process['process_id'] for address, process in itertools.chain.from_iterable([queue_details['stream_heartbeats'].items() for queue_details in connections.values()])}
            except zmq.Again:
                time.sleep(1)
            if last_send_time + datetime.timedelta(seconds=10) < datetime.datetime.now():
                Message(None, intersection_connection_info).send(update_connection_sock, producer_uuid=self.process_uuid)
                last_send_time = datetime.datetime.now()
