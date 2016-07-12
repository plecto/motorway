from motorway.grouping import HashRingGrouper, RandomGrouper, GroupingValueMissing, SendToAllGrouper


class GrouperMixin(object):
    def get_grouper(self, grouper_name):
        if grouper_name == "HashRingGrouper":
            return HashRingGrouper
        elif "SendToAllGrouper":
            return SendToAllGrouper
        else:
            return RandomGrouper


class SendMessageMixin(object):

    def send_message(self, message, process_id, time_consumed=None, sender=None, control_message=True):
        try:
            socket_addresses = self.get_grouper(self.send_grouper)(
                self.send_socks.keys()
            ).get_destinations_for(message.grouping_value)
        except GroupingValueMissing:
            raise GroupingValueMissing("Message '%s' provided an invalid grouping_value: '%s'" % (message.content, message.grouping_value) )
        for destination in socket_addresses:
            message.send(
                self.send_socks[destination],
                process_id
            )
            if self.controller_sock and control_message:
                message.send_control_message(
                    self.controller_sock,
                    time_consumed=time_consumed,
                    process_name=process_id,
                    destination_endpoint=destination,
                    sender=sender
                )