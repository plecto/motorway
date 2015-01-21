from motorway.grouping import HashRingGrouper, RandomGrouper


class GrouperMixin(object):
    def get_grouper(self, grouper_name):
        if grouper_name == "HashRingGrouper":
            return HashRingGrouper
        else:
            return RandomGrouper