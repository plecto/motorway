import mock


class ZMQSockMock(object):
    def __init__(self, control_messages):
        self.control_messages = control_messages

    def send_json(self, message):
        self.control_messages.append(message)


def override_process_name(new_name):
    return mock.patch("multiprocessing.current_process", new=type("mock", (object, ), {'name': new_name}))