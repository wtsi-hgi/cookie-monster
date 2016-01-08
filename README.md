[![Build Status](https://travis-ci.org/wtsi-hgi/cookie-monster.svg)](https://travis-ci.org/wtsi-hgi/cookie-monster)

# Cookie Monster

## Setup
### Rules
Rules have a matching criteria to which cookies are compared to determine if any action should be taken. If matched, 
the rule specifies an action for the cookie that can indicate that notification receivers should be informed and whether
further processing of the cookie is required. The order in which rules are applied is determined by their priority.

#### Changing rules on-the-fly
If ``RuleSource`` is being used by your ``ProcessorManager`` to attain the rules that are followed by ``Processor``
instances, it is possible to dynamically changes the rules used by the Cookie Monster for future jobs (jobs already 
running will continue to use the set of rules that they had when they were started).

The following code illustrates how a rule is defined and registered. If appropriate, the code can be inserted into an 
existing rule file. Alternatively, it can be added to a new file in the rules directory, with a name matching the
format: ``*.rule.py``. Rule files can be put into subdirectories.
```python
from cookiemonster import Cookie, Notification, Rule, RuleAction
from hgicommon.mixable import Priority
from hgicommon.data_source import register

def _matches(cookie: Cookie) -> bool:
    return "my_study" in cookie.path
        
def _generate_action(cookie: Cookie) -> RuleAction:
    return RuleAction([Notification("everyone", data=cookie.path, sender="this_rule")], True)

_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _generate_action, _priority)
register(_rule)
```

To delete a pre-existing rule, delete the file containing it or remove the relevant call to ``register``. To modify a 
rule, simply change its code and it will be updated on save.


### Cookie Enrichments
If all the rules have been applied against a cookie and none of them indicated in their action that no further
processing is required, cookie "enrichment" loaders can be used to load more information about a cookie.

#### Changing enrichment loaders on-the-fly
Similarly to rules, the enrichment loaders, used to increase the knowledge of a cookie, can be changed during execution.
Files containing enrichment loaders must have a name matching the format: ``*.loader.py``.
```python
from cookiemonster import EnrichmentLoader, Cookie, Enrichment
from hgicommon.mixable import Priority
from hgicommon.data_source import register

def _can_enrich(cookie: Cookie) -> bool:
    return "my_data_source" in [enrichment.source for enrichment in cookie.enrichments]
    
def _load_enrichment(cookie: Cookie) -> Enrichment:
    return my_data_source.load_more_information_about(cookie.path)

_priority = Priority.MAX_PRIORITY

_enrichment_loader = EnrichmentLoader(_can_enrich, _load_enrichment, _priority)
register(_enrichment_loader)
```


## How to develop
### Testing
#### Locally
To run the tests, use ``./scripts/run-tests.sh`` from the project's root directory. This script will use ``pip`` to 
install all requirements for running the tests (use `virtualenv` if necessary).

#### Using Docker
From the project's root directory:
```bash
$ docker build -t wtsi-hgi/cookie-monster/test -f docker/tests/Dockerfile .
$ docker run wtsi-hgi/cookie-monster/test
```