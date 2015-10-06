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

def fetch_file_metadata(fpath):
    metaquery_results = baton.BatonAPI.list_file_metadata(fpath)
    print "Metadata query results: " + str(metaquery_results)
    try:
        fmeta = baton_results_processor.from_metalist_results_to_avus(metaquery_results)  # this is a dict of key = fpath, value = dict({'avus':[], 'checksum':str})
    except ValueError as e:
        raise ValueError("Couldn't retrieve the iRODS file metadata: " + str(metaquery_results))
    return fmeta

def fetch_all_files_metadata(fpaths):
    errors = []
    fmeta_list = []
    for fpath in fpaths:
        try:
            fmeta = fetch_file_metadata(fpath)
        except ValueError as e:
            errors.append(e.message)
        else:
            fmeta_list.append(fmeta)
    return fmeta_list, errors

def apply_fpath_filters(fpaths, filters):
    if 'files_by_extension' in filters:
        return apply_filter.apply_file_extension_filter(fpaths, filters['files_by_extension'])
    return fpaths


def apply_metadata_filters(fmeta_list, filters):
    print "FILES BEFORE FILTERS BY METADATAAAAAAA: " + "\n".join(str(f) for f in fmeta_list)
    if 'target' in filters:
        fmeta_list = apply_filter.apply_target_filter(fmeta_list, filters['target'])
        print "Target files after filtering: " + "\n".join(str(fmeta) for fmeta in fmeta_list)
    if 'manual_qc' in filters:
        fmeta_list = apply_filter.apply_manual_qc_filter(fmeta_list, filters['target'])
        print "Applied the manual qc filter => nr of files that passed the filter are: " + str(len(fmeta_list))
    if 'reference_genome' in filters:
        fmeta_list = apply_filter.apply_reference_filter(fmeta_list, filters['reference_genome'])
        for f in fmeta_list:
            print "REF FILTERS APPLIED - result = " + str(f)
        print "Nr of files after reference filtering: " + str(len(fmeta_list)) + " and files: "+ "\n".join(str(f) for f in fmeta_list)
    return fmeta_list

def process_fpaths(fpaths):
    filters = filter_parser.parse_filter_file('filters.txt')
    filtered_paths = apply_fpath_filters(fpaths, filters)

    fmeta_list, errors = fetch_all_files_metadata(filtered_paths)
    if not fmeta_list:
        return errors
    if errors:
        # decide what to do when there are **SOME** errors
        pass
    filtered_fmeta_list = apply_metadata_filters(fmeta_list, filters)
    return filtered_fmeta_list


def read_file(fpath):
    fpaths = []
    fh = open(fpath)
    for line in fh:
        fpaths.append(line.strip())
    fh.close()
    return fpaths


def main():
    #fpaths = read_file('query_short.txt')
    fpaths = read_file('query-out.txt')
    print "LENGTH: " + str(len(fpaths))
    process_fpaths(fpaths)
    #process_fpaths(["/seq/16063/16063_6.cram", "/seq/15767/15767_7#64.cram", '/seq/17426/17426_8#7.cram'])

main()