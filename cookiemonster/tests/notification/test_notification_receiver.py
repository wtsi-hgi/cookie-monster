import unittest

from cookiemonster.notifications.notification_receiver import NotificationReceiverSource


class TestNotificationReceiverSource(unittest.TestCase):
    """
    Tests for `NotificationReceiverSource`.
    """
    def setUp(self):
        self.source = NotificationReceiverSource("/")

    def test_is_data_file_when_is(self):
        self.assertTrue(self.source.is_data_file("/my/file.notification_receiver.py"))

    def test_is_data_file_when_is_not(self):
        self.assertFalse(self.source.is_data_file("/my/file.py"))


if __name__ == "__main__":
    unittest.main()
