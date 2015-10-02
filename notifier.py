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

import baton_wrapper as baton
import baton_results_processor
from filtering import filter_parser, filter_type, apply_filter

def fetch_irods_metadata(fpath):
    metaquery_results = baton.BatonAPI.list_file_metadata(fpath)
    fmeta = baton_results_processor.from_metalist_results_to_avus(metaquery_results)  # this is a dict of key = fpath, value = dict({'avus':[], 'checksum':str})
    return fmeta


def process_fpaths(fpaths):
    filters = filter_parser.parse_filter_file('filters.txt')
    filtered_paths = fpaths
    if 'files_by_extension' in filters:
        filtered_paths = apply_filter.apply_file_extension_filter(fpaths, filters['files_by_extension'])

    print "Files remaining after filtering by extension:"+ str(len(filtered_paths)) +" and files: " + str(filtered_paths)

    fmeta_list = []
    for fpath in filtered_paths:
        fmeta_list.append(fetch_irods_metadata(fpath))
    if 'reference_genome' in filters:
        filtered_fmeta_list = apply_filter.apply_reference_filter(fmeta_list, filters['reference_genome'])
        for f in filtered_fmeta_list:
            print "REF FILTERS APPLIED - result = " + str(f)
        print "Nr of files after reference filtering: " + str(len(filtered_fmeta_list))

def read_file(fpath):
    fpaths = []
    fh = open(fpath)
    for line in fh:
        fpaths.append(line.strip())
    fh.close()
    return fpaths

def main():
    fpaths = read_file('query_short.txt')
    print "LENGTH: " + str(len(fpaths))
    process_fpaths(fpaths)
    #process_fpaths(["/seq/16063/16063_6.cram", "/seq/15767/15767_7#64.cram", '/seq/17426/17426_8#7.cram'])

main()