"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
from typing import Callable

from cookiemonster import Notification
from cookiemonster.common.resource_accessor import ResourceAccessor, ResourceAccessorContainer


class NotificationReceiver(ResourceAccessorContainer):
    """
    Receiver of notifications.
    """
    def __init__(self, receive: Callable[[Notification, ResourceAccessor], None]):
        """
        Constructor.
        :param receive: method proxied by `receive`
        """
        super().__init__()
        self._receive = receive

    def receive(self, notification: Notification):
        """
        Receive notification. It is this method's responsibility to determine if it is interested in the given
        notification (the cookie monster informs all notification receivers about all notifications).
        :param notification: the notification to give to the receiver
        """
        self._receive(notification, self.resource_accessor)
