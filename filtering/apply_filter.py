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

This file has been created on Sep 29, 2015.
"""

from com import utils

# FILES_BY_EXTENSION = cram
#                      bam
#
# TARGET = 1
# MANUAL_QC = 1


def apply_file_extension_filter(fpaths, file_filter):
    if file_filter.type == 'INCLUDE':
        return [fpath for fpath in fpaths if utils.extract_file_extension(fpath) in file_filter.values]
    elif file_filter.type == 'EXCLUDE':
        return [fpath for fpath in fpaths if utils.extract_file_extension(fpath) not in file_filter.values]
    else:
        raise ValueError("Filter type can only be INCLUDE/EXCLUDE.")

def apply_target_filter(fmeta_list, file_filter):
    if file_filter.type == 'INCLUDE':
        return [fmeta for fmeta in fmeta_list if fmeta.target == file_filter.values]
    elif file_filter.type == 'EXCLUDE':
        return [fmeta for fmeta in fmeta_list if fmeta.target != file_filter.values]
    else:
        raise ValueError("Filter type can only be INCLUDE/EXCLUDE.")


def apply_manual_qc_filter(fmeta_list, file_filter):
    if file_filter.type == 'INCLUDE':
        return [fmeta for fmeta in fmeta_list if fmeta.manual_qc == file_filter.values]
    elif file_filter.type == 'EXCLUDE':
        return [fmeta for fmeta in fmeta_list if fmeta.manual_qc != file_filter.values]
    else:
        raise ValueError("Filter type can only be INCLUDE/EXCLUDE.")


def is_wanted_reference(wanted_reference, actual_reference_file):
    if actual_reference_file.find(wanted_reference) == -1:
        return False
    return True


def apply_reference_filter(fmeta_list, file_filter):
    print "BFORE APPLYING THE REFERENCE FILTER: " + str(file_filter) + " and length of fmeta list: " + str(len(fmeta_list))
    results = []
    if file_filter.type == 'INCLUDE':
        for fmeta in fmeta_list:
            filter_refs = [file_filter.values] if type(file_filter.values) != list else file_filter.values
            for ref in filter_refs:
                print "REF IN file filter: " + str(ref) + " and file reference = " + str(fmeta.reference)
                if fmeta.reference and fmeta.reference.find(ref) != -1:
                    results.append(fmeta)
    elif file_filter.type == 'EXCLUDE':
        for fmeta in fmeta_list:
            filter_refs = [file_filter.values] if type(file_filter.values) != list else file_filter.values
            for ref in filter_refs:
                if fmeta.reference and not is_wanted_reference(ref, fmeta.reference):
                    #fmeta.reference.find(ref) == -1:
                    results.append(fmeta)
    return results



