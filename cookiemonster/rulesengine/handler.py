"""
Copyright (C) 2015  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of cookie-monster

cookie-monster is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

This file has been created on Nov 06, 2015.
"""
from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import FileUpdate


class FileUpdatesHandler:
    """
    This class is for managing the processing of file updates.
    """
    def __init__(self):
        pass

    def handle_update(self, file_update: FileUpdate):
        """
        This method takes an update and decides what to do with it next.
        :param update:
        :return:
        """
        pass

    def handle_batch_updates(self, file_updates_list: FileUpdateCollection):
        """
        This method takes a collection of updates and process it in the sense that it decides what to do further.
        :param file_updates_list:
        :return:
        """
        pass





