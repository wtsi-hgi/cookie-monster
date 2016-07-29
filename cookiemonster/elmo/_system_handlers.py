"""
System/Python API Handlers
==========================
Exportable classes: `SystemHandlers`

SystemHandlers
--------------
Method handlers for system/Python runtime information:

* `GET_thread_dump` GET handler for thread debugging information

Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Christopher Harrison <ch12@sanger.ac.uk>

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
import sys
import traceback

from cookiemonster.elmo._handler_injection import DependencyInjectionHandler


class SystemHandlers(DependencyInjectionHandler):
    """ Handler functions for system/Python runtime """
    def GET_thread_dump(self, **kwargs):
        return {
            thread_id: [
                {
                    'file': filename,
                    'line': lineno,
                    'in':   name,
                    'code': code
                }
                for filename, lineno, name, code in traceback.extract_stack(stack)
            ]
            for thread_id, stack in sys._current_frames().items()
        }
