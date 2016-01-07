from cookiemonster.common.models import Notification
from cookiemonster.notifier.notifier import Notifier


class PrintingNotifier(Notifier):
    """
    A `Notifier` implementation that prints notifications.
    """
    def do(self, notification: Notification):
        print("Informing \"%s\" with the data \"%s\"" % (notification.external_process_name, notification.data))
