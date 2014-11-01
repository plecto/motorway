from unittest import TestCase
import datetime
from motorway.controller import ControllerIntersection
from motorway.messages import Message
from motorway.tests.sample_pipeline import SampleWordCountPipeline
from motorway.tests.utils import override_process_name, ZMQSockMock
from freezegun import freeze_time


class ControllerTestCase(TestCase):

    # Utilities to test the controller

    def setUp(self):
        self.controller = ControllerIntersection({}, {}, web_server=False)
        self.control_messages = []
        self.controller_sock = ZMQSockMock(self.control_messages)

    def get_control_messages(self):
        messages = [Message.from_message(self.control_messages.pop(0), self.controller_sock) for msg in self.control_messages]
        return messages

    def run_controller_process_method(self):
        return list(
            self.controller.process(self.get_control_messages())
        )  # Remember list(), to evaluate a generator

    # Tests

    def test_histogram_success(self):
        with freeze_time("2014-01-10 12:00:01"):
            with override_process_name("TestRamp-0"):
                msg_in_ramp = Message(1337, "split this string please")
                msg_in_ramp.send_control_message(self.controller_sock, datetime.timedelta(seconds=2))
            self.run_controller_process_method()
            self.controller.update()
            with override_process_name("TestIntersection-0"):
                msg_in_intersection = Message.from_message(msg_in_ramp._message(), self.controller_sock)
                msg_in_intersection.ack()
            self.run_controller_process_method()
            self.controller.update()
        self.assertEqual(self.controller.process_statistics['TestRamp-0']['histogram'][0]['success_count'], 1)
        with freeze_time("2014-01-10 12:59:01"):
            self.controller.update()
        self.assertEqual(self.controller.process_statistics['TestRamp-0']['histogram'][0]['success_count'], 0)

    def test_histogram_error(self):
        with freeze_time("2014-01-10 12:00:01"):
            with override_process_name("TestRamp-0"):
                msg_in_ramp = Message(1337, "split this string please")
                msg_in_ramp.send_control_message(self.controller_sock, datetime.timedelta(seconds=2))
            self.run_controller_process_method()
            self.controller.update()
            with override_process_name("TestIntersection-0"):
                msg_in_intersection = Message.from_message(msg_in_ramp._message(), self.controller_sock)
                msg_in_intersection.fail("some error message", False)
            self.run_controller_process_method()
            self.controller.update()
        self.assertEqual(self.controller.process_statistics['TestRamp-0']['histogram'][0]['error_count'], 1)
        with freeze_time("2014-01-10 12:59:01"):
            self.controller.update()
        self.assertEqual(self.controller.process_statistics['TestRamp-0']['histogram'][0]['error_count'], 0)

    def test_frequency(self):
        msg = Message(1337, "split this string please")
        with override_process_name("TestRamp-0"):
            msg.send_control_message(self.controller_sock, datetime.timedelta(seconds=2))
        self.run_controller_process_method()
        self.controller.update()
        self.assertEqual(self.controller.process_statistics['TestRamp-0']['frequency'][2], 1)

    def test_waiting(self):
        msg_in_ramp = Message(1337, "split this string please")
        with override_process_name("TestRamp-0"):
            msg_in_ramp.send_control_message(self.controller_sock, datetime.timedelta(seconds=2))
        self.run_controller_process_method()
        self.controller.update()
        self.assertEqual(self.controller.process_statistics['TestRamp-0']['waiting'], 1)
        with override_process_name("TestIntersection-0"):
            msg_in_intersection = Message.from_message(msg_in_ramp._message(), self.controller_sock)
            msg_in_intersection.ack()
        self.run_controller_process_method()
        self.controller.update()
        self.assertEqual(self.controller.process_statistics['TestRamp-0']['waiting'], 0)

    def test_process_dict(self):
        pipeline = SampleWordCountPipeline()
        pipeline.definition()  # Add processes
        self.controller = ControllerIntersection(pipeline._stream_consumers, {}, web_server=False)
        for process in ['WordRamp-0', 'SentenceSplitIntersection-0', 'WordCountIntersection-0']:
            self.assertIn(
                process,
                self.controller.process_statistics,
                msg="%s was not found in controller.process_statistics" % process
            )

    def test_message_timeout(self):
        with freeze_time("2014-01-10 12:00:01"):
            with override_process_name("TestRamp-0"):
                msg_in_ramp = Message(1337, "split this string please")
                msg_in_ramp.send_control_message(self.controller_sock, datetime.timedelta(seconds=2))
            self.run_controller_process_method()
            self.controller.update()
        self.assertEqual(self.controller.process_statistics['TestRamp-0']['waiting'], 1)
        with freeze_time("2014-01-10 12:59:01"):
            self.controller.update()
            self.assertEqual(self.controller.process_statistics['TestRamp-0']['waiting'], 0)
            self.run_controller_process_method()
            self.assertEqual(self.controller.process_statistics['TestRamp-0']['histogram'][59]['error_count'], 1)




