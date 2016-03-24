"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
from cookiemonster.common.resource_accessor import ResourceAccessor
from hgicommon.data_source import register
from hgicommon.mixable import Priority

from cookiemonster import Cookie, Rule, RuleAction


def _matches(cookie: Cookie, resource_accessor: ResourceAccessor) -> bool:
    return False


def _generate_action(cookie: Cookie, resource_accessor: ResourceAccessor) -> RuleAction:
    assert False


_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _generate_action, _priority)
register(_rule)
