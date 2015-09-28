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

This file has been created on Sep 22, 2015.
"""

from collections import defaultdict, namedtuple
import json

MetaAVU = namedtuple('MetaAVU', ['attribute', 'value'])

def from_metaquery_results_to_fpaths_and_avus(search_results_json):
    """
        This method takes as parameter the json result of a metaquery containing avus and checksum,
        and turns the json into a dict having as key a fpath, and as value: dict of
        {'avus' : [MetaAVU(), MetaAVU()], 'checksum' : 'the_result_of_ichksum'}
    :param search_results_json:
    :param filters: optional (not implemented yet)
    :return: dict key = fpath, value = {'avus' : [MetaAVU(), MetaAVU()], 'checksum' : 'the_result_of_ichksum'}
    """
    do_avus = []
    data_dict = json.loads(search_results_json)
    for do_item, do_item_val in data_dict.items():
        if do_item == 'avus':
            for avu in do_item_val:
                avu_obj = MetaAVU(attribute=str(avu['attribute']), value=str(avu['value']))  # MetaAVU = namedtuple('MetaAVU', ['attribute', 'value'])    # list of attribute-value tuples
                do_avus.append(avu_obj)
        # elif do_item == 'checksum':
        #     do_checksum = str(do_item_val)
        # TODO: sometimes there is an error here instead of a list of avus !!!!
    return do_avus



