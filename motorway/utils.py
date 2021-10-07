import decimal
from json import JSONEncoder
import datetime
from isodate import duration_isoformat, datetime_isoformat
import zmq
import socket

ramp_result_stream_name = lambda ramp_class_name: "_ramp_result_%s" % ramp_class_name


def percentile_from_dict(D, P):
    """
    Find the percentile of a list of values

    @parameter N - A dictionary, {observation: frequency}
    @parameter P - An integer between 0 and 100

    @return - The percentile of the values.
    """
    assert 0 < P <= 100, "Percentile must be in range (0, 100)"
    N = sum(D.values())
    P = float(P)/100
    n = int(N) * P

    i = 1  # The formula returns a 1-indexed number
    observation = None
    for observation in sorted(D.keys()):
        if i >= n:
            return observation
        i += D[observation]
    return observation
    # raise AssertionError("Didn't find percentile")


class DateTimeAwareJsonEncoder(JSONEncoder):
    def default(self, o):
        if type(o) == datetime.timedelta:
            return duration_isoformat(o)
        elif type(o) == datetime.datetime:
            return datetime_isoformat(o)
        elif isinstance(o, decimal.Decimal):
            return float(o)
        return super(DateTimeAwareJsonEncoder, self).default(o)


def set_timeouts_on_socket(scket):
    """
    Applies a set of options to a socket.

    IMPORTANT: These options should be applied before binding/connecting
    to a socket, as they will otherwise have no effect!

    :param scket: 'context.socket'-instance
    """
    # Duration before returning AGAIN-exception when receiving messages
    scket.RCVTIMEO = 10000
    # Duration before returning AGAIN-exception when trying to send messages
    scket.SNDTIMEO = 10000
    # How long to keep messages in memory after a socket disconnects
    scket.LINGER = 1000


def get_connections_block(queue, refresh_connection_socket, limit=100, existing_connections=None):
    """
    Will keep refreshing the available connections until the selected queue is available within
    the dictionary of returned connections, or if the max number of retries 'limit' is reached.

    :param queue: str, name of the queue to search for among all connections
    :param refresh_connection_socket: str, TCP-address of the ConnectionIntersection
    :param limit: int, number of attempts to receive connections
    :param existing_connections: (Optional) dict, containing existing connections to include
    :return: dict, containing all connections fetched from the ConnectionIntersection or 'existing_connections'
    """
    i = 0
    connections = existing_connections if existing_connections else {}
    while queue not in connections and i < limit:
        try:
            connections = refresh_connection_socket.recv_json()
        except zmq.Again:
            pass
        i += 1
    return connections


def get_ip():
    # From http://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 0))
        ip = s.getsockname()[0]
    except:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip