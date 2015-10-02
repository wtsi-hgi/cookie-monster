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

This file has been created on Oct 02, 2015.
"""

class FileMetadata(object):

    def __init__(self, fpath=None, manual_qc=None, target=None, reference=None):
        self.fpath = fpath
        self.manual_qc = manual_qc
        self.target = target
        self.reference = reference

    def __str__(self):
        return "File: " + str(self.fpath) + ", manual_qc = " + str(self.manual_qc) + ", target = " + str(self.target) + ", reference = " + str(self.reference)

    def __repr__(self):
        return self.__str__()