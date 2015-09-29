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
import os

#FILTERED_OUT_EXT = ['stats', 'bamcheck', 'flagstat', 'txt', 'seqchksum', 'json', 'crai', 'bai']

def extract_file_extension(fpath):
    if not fpath:
        return None
    _, tail = os.path.split(fpath)
    _, ext = os.path.splitext(tail)
    return ext[1:].strip()


def fetch_irods_metadata(fpath):
    metaquery_results = baton.BatonAPI.list_file_metadata(fpath)
    fpath_avus = baton_results_processor.from_metalist_results_to_avus(metaquery_results)  # this is a dict of key = fpath, value = dict({'avus':[], 'checksum':str})
    print str(fpath_avus)

def apply_filters(fpaths):
    useful_files = []
    for fpath in fpaths:
        file_ext = extract_file_extension(fpath)
        if file_ext not in FILTERED_OUT_EXT:
            useful_files.append(fpath)
    return useful_files




def main():
    #fetch_irods_metadata('/seq/17426/17426_8#7.cram')
    #fetch_irods_metadata('/seq/15767/15767_7#64.cram')
    fetch_irods_metadata("/seq/16063/16063_6.cram")

main()