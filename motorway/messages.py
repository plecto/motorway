import json
import multiprocessing
import traceback
import uuid
import datetime
from isodate import duration_isoformat
from motorway.utils import DateTimeAwareJsonEncoder
import logging


class Message(object):
    """
    :param ramp_unique_id: the unique message ID delivered back upon completion to the ramp
    :param content: any json serializable content
    :param grouping_value: String that can be used for routing messages consistently to the same receiver
    :return:
    """
    FAIL = -1
    SUCCESS = 0

    def __init__(self, ramp_unique_id, content=None, ack_value=None, controller_queue=None, grouping_value=None,
                 error_message=None, process_name=None, producer_uuid=None, destination_endpoint=None,
                 destination_uuid=None):
        self.ramp_unique_id = ramp_unique_id
        self.content = content
        if not ack_value:
            ack_value = uuid.uuid4().int
        self.ack_value = ack_value
        self.controller_queue = controller_queue
        self.grouping_value = grouping_value
        self.error_message = error_message
        self.process_name = process_name
        self.producer_uuid = producer_uuid
        self.destination_endpoint = destination_endpoint
        self.destination_uuid = destination_uuid
        self.init_time = datetime.datetime.now()

    @classmethod
    def new(cls, message, content, grouping_value=None, error_message=None):
        """
        Creates a new message, based on an existing message. This has the consequence that it will be tracked together
            and the tap will not be notified until every message in the chain is properly ack'ed.

        :param message: Message instance, as received by the intersection
        :param content: Any value that can be serialized into json
        :param grouping_value: String that can be used for routing messages consistently to the same receiver
        """
        return cls(ramp_unique_id=message.ramp_unique_id, content=content, grouping_value=grouping_value,
                   error_message=error_message, producer_uuid=message.producer_uuid)

    @classmethod
    def from_message(cls, message, controller_queue, process_name=None):
        """

        :param message: Message dict (converted from JSON)
        :param controller_queue:
        :param process_name: UUID of the process processing this message (as string)
        :return:
        """
        # assert type(message) is dict, "message (%s) should be dict" % message
        if type(message) is not dict:
            logging.error("Expected type dict, got type %s - message: %s", type(message), message)
        message['process_name'] = process_name
        # assert 'producer_uuid' in message, "missing uuid %s" % message
        return cls(controller_queue=controller_queue, **message)

    def _message(self):
        return {
            'content': self.content,
            'ramp_unique_id': self.ramp_unique_id,
            'ack_value': self.ack_value,
            'grouping_value': self.grouping_value,
            'producer_uuid': self.producer_uuid
        }

    def as_json(self):
        return json.dumps(self._message(), cls=DateTimeAwareJsonEncoder)

    def send(self, queue, producer_uuid=None):
        if producer_uuid and not self.producer_uuid:  # Check if provided and we didn't get one already
            self.producer_uuid = producer_uuid
        elif not self.producer_uuid:
            assert self.producer_uuid
        queue.send(
            self.as_json()
        )

    def send_control_message(self, controller_queue, time_consumed=None, process_name=None, destination_endpoint=None,
                             destination_uuid=None, sender=None):
        """
        Control messages are notifications that a new message have been created, so the controller can keep track of
        this particular message and let the ramp know once the entire tree of messages has been completed.

        This is called implicitly on yield Message(_id, 'message')

        :param process_name: UUID of the process processing this message (as string)
        """
        content = {
            'process_name': process_name,
            'msg_type': 'new_msg',
        }
        if not self.producer_uuid:
            raise Exception("Cannot send control message without producer UUID")
        if time_consumed:
            # Ramps provide time consumed, since we don't know the "start time" like in a intersection
            # where it's clear when the message is received and later 'acked' as the last action
            content['duration'] = duration_isoformat(time_consumed)
            content['sender'] = sender
        controller_queue.send_json({
            'ramp_unique_id': self.ramp_unique_id,
            'ack_value': self.ack_value,
            'content': content,
            'producer_uuid': self.producer_uuid,
            'destination_endpoint': destination_endpoint,
            'destination_uuid': destination_uuid
        })

    def ack(self, time_consumed=None):
        """
        Send a message to the controller that this message was properly processed
        """
        self.controller_queue.send_json({
            'ramp_unique_id': self.ramp_unique_id,
            'ack_value': self.ack_value,
            'content': {
                'process_name': self.process_name,
                'msg_type': 'ack',
                'duration': duration_isoformat(time_consumed or (datetime.datetime.now() - self.init_time))
            },
            'producer_uuid': self.producer_uuid,
            'destination_uuid': self.producer_uuid
        })

    def fail(self, error_message="", capture_exception=True):
        """
        Send a message to the controller that this message failed to process
        """
        self.controller_queue.send_json({
            'ramp_unique_id': self.ramp_unique_id,
            'ack_value': -1,
            'content': {
                'process_name': self.process_name,
                'msg_type': 'fail',
                'duration': duration_isoformat(datetime.datetime.now() - self.init_time),
                'message_content': self.content
            },
            'producer_uuid': self.producer_uuid,
            'destination_uuid': self.producer_uuid,
            'error_message': error_message if not capture_exception else traceback.format_exc(),
        })

    def __repr__(self):
        return "<Message: %s> %s" % (self.ramp_unique_id, self.content)



