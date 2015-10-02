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
from com import exceptions
from filter_type import FilesFilter

def parse_section(config, section):
    section_fields = {}
    if config.has_section(section):
        section_opts = config.items(section)
        for opt in section_opts:
            opt_name = opt[0]
            opt_value = opt[1]
            if opt[1].find('\n') != -1:
                opt_value = opt_value.split('\n')
            section_fields[opt_name] = opt_value
    return section_fields

def check_filters_correct(incl_filters, excl_filters):
    if set(incl_filters).intersection(set(excl_filters)):
        raise ValueError("The filters aren't correct, you can't include one filter and exclude it at the same time.")

def change_filters_format(incl_filters, excl_filters):
    filters = {}
    print str(incl_filters)
    print str(excl_filters)
    for filter_name, val in incl_filters.items():
        filters[filter_name] = FilesFilter(filter_name, val, 'INCLUDE')
    for filter_name, val in excl_filters.items():
        filters[filter_name] = FilesFilter(filter_name, val, 'EXCLUDE')
    return filters

def parse_filter_file(fpath):
    config = ConfigParser.ConfigParser()
    config.read(fpath)
    included_filter = parse_section(config, 'INCLUDE')
    excluded_filter = parse_section(config, 'EXCLUDE')
    try:
        check_filters_correct(included_filter, excluded_filter)
    except ValueError as e:
        raise exceptions.WrongConfigFile(fpath, e.message)
    else:
        filters = change_filters_format(included_filter, excluded_filter)
        return filters


# incl, excl = parse_filter_file('/nfs/users/nfs_i/ic4/Projects/cookie-monster/cookie-monster/filters.txt')
# print "included" + str(vars(incl))
# print "excluded" + str(vars(excl))






