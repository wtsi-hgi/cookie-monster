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


"""

import functools
import inspect


def check_args_not_none(funct):
    @functools.wraps(funct)
    def wrapper(*args, **kwargs):
        func_args = inspect.getcallargs(funct, *args, **kwargs)
        none_args = [(arg, val) for arg, val in func_args.items() if val is None]
        if none_args:
            msg = "None arguments have been provided for this function: "+str(func_args)
            raise ValueError(msg)
        return funct(*args, **kwargs)
    return wrapper


def one_argument_only(funct):
    """
    This function decorator is used to annotate functions/methods that take a list of optional
    parameters and should have exactly one non empty parameter out of all the list.
    """
    @functools.wraps(funct)
    def wrapper(*args, **kwargs):
        func_args = inspect.getcallargs(funct, *args, **kwargs)
        non_empty_args = [(arg, val) for arg, val in func_args.items() if val is not None]
        if len(non_empty_args) != 1:
            msg = "This function should be called with exactly 1 parameter from the optional parameters list"
            raise ValueError(msg)
        return funct(*args, **kwargs)
    return wrapper
