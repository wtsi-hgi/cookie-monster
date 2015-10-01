"""
Copyright (C) 2014  Genome Research Ltd.

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

This file has been created on Nov 14, 2014.
"""

import json
from sqlalchemy.ext.declarative import DeclarativeMeta

from com import wrappers
from seqscape import normalization

# from metadata-check.com import
# from serapis.seqscape import normalization


@wrappers.check_args_not_none
def normalize_sample_data(samples):
    for sample in samples:
        if sample.taxon_id == 9606:     # If it's human
            sample.organism = normalization.normalize_human_organism(sample.organism)
        if sample.country_of_origin:
            sample.country_of_origin = normalization.normalize_country(sample.country_of_origin)
    return samples
        

@wrappers.check_args_not_none
def remove_empty_fields(obj_as_dict):
    """ 
        This method receives a dict representing the fields of an object
        and their values and removes the key-value pairs that have empty values
        where empty means None value, empty string, or 'Not specified'.
        Parameters
        ----------
        obj_as_dict: dict
            A dict representing the fields of an object (__dict__ of an obj)
        Returns
        -------
        result_dict: dict
            A new dict containing only the fields that are not empty
    """
    result = {}
    for field_name, field_val in obj_as_dict.iteritems():
        if not field_val in [None, '', ' ', 'Not specified', ]:
            result[field_name] = field_val
    return result


@wrappers.check_args_not_none
def to_json(model):
    """ Returns a JSON representation of an SQLAlchemy-backed object.
    """
    json_repr = {}
    for col in model._sa_class_manager.mapper.mapped_table.columns:
        if col.name != 'is_current':
            json_repr[col.name] = getattr(model, col.name)
    return json.dumps([json_repr])


@wrappers.check_args_not_none
def to_primitive_types(model):
    if isinstance(model.__class__, DeclarativeMeta):
        fields = {}
        for field in [x for x in dir(model) if not x.startswith('_') and x != 'metadata']:
            data = model.__getattribute__(field)
            fields[field] = data
        return fields
    

# Not used yet, but potentially useful
class AlchemyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            # an SQLAlchemy class
            fields = {}
            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                data = obj.__getattribute__(field)
                try:
                    json.dumps(data) # this will fail on non-encodable values, like other classes
                    fields[field] = data
                except TypeError:
                    fields[field] = None
            return fields
    
        return json.JSONEncoder.default(self, obj)


