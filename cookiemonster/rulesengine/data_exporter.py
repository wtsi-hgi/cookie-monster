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

from cookiemonster.common.models import FileUpdate, FileUpdateCollection

class DataExporter:
    """
    This class exports the file updates after they've been analyzed to the corresponding queues.
    """

    def __init__(self):
        pass

    def export_relevant_data(self, file_update: FileUpdate):
        """
        This method exports the updates that are of interest to the corresponding queue.
        :param file_update:
        :return:
        """
        pass

    def export_uncertain_data(self, file_update: FileUpdate):
        """
        This method exports to the corresponding queue the data that we can't categorize.
        :param file_update:
        :return:
        """
        pass

    def trash_data(self, file_update: FileUpdate):
        """
        Probably we don't need this, but maybe at the beginning for testing reasons, we may want to save
        the trash as well, just to make sure that it's actually trash and we don't miss things out.
        :param file_update:
        :return:
        """
        pass




