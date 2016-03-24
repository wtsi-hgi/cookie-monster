"""
Dependency Injection Handler
============================
Exportable classes: `DependencyInjectionHandler`

DependencyInjectionHandler
--------------------------
Superclass for data source handlers that allow dependency injection
through the `inject_dependency`.

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

class DependencyInjectionHandler(object):
    """ Dependency injection superclass for handlers """
    def inject_dependency(self, dependency:object):
        self._dependency = dependency
