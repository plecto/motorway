from motorway.grouping import HashRingGrouper, RandomGrouper, GroupingValueMissing


class GrouperMixin(object):
    def get_grouper(self, grouper_name):
        if grouper_name == "HashRingGrouper":
            return HashRingGrouper
        else:
            return RandomGrouper


class SendMessageMixin(object):

    def send_message(self, message, process_id, time_consumed=None, sender=None):
        try:
            socket_address = self.get_grouper(self.send_grouper)(
                self.send_socks.keys()
            ).get_destination_for(message.grouping_value)
        except GroupingValueMissing:
            raise GroupingValueMissing("Message '%s' provided an invalid grouping_value: '%s'" % (message.content, message.grouping_value) )
        message.send(
            self.send_socks[socket_address],
            process_id
        )
        if self.controller_sock:
            message.send_control_message(
                self.controller_sock,
                time_consumed=time_consumed,
                process_name=process_id,
                destination_endpoint=socket_address,
                sender=sender
            )