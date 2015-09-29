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

This file has been created on Sep 28, 2015.
"""

import ConfigParser
from filter_type import FilterOptions

def parse_section(config, section):
    section_fields = FilterOptions()
    if config.has_section(section):
        section_opts = config.items(section)
        for opt in section_opts:
            opt_name = opt[0]
            opt_value = opt[1]
            if opt[1].find('\n') != -1:
                opt_value = opt_value.split('\n')
            setattr(section_fields, opt_name, opt_value)
    return section_fields


def parse_filter_file(fpath):
    config = ConfigParser.ConfigParser()
    config.read(fpath)
    included_filter = parse_section(config, 'INCLUDE')
    excluded_filter = parse_section(config, 'EXCLUDE')
    return included_filter, excluded_filter


incl, excl = parse_filter_file('/nfs/users/nfs_i/ic4/Projects/cookie-monster/cookie-monster/filters.txt')
print "included" + str(vars(incl))
print "excluded" + str(vars(excl))


# config.read('/nfs/users/nfs_i/ic4/Projects/cookie-monster/cookie-monster/filters.txt')

# config.sections()
#config.has_section('INCLUDAAE')#
# if config.has_option('INCLUDE', 'TARGET'):
#     target = config.get('INCLUDE', 'TARGET')






