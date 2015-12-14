[![Build Status](https://travis-ci.org/wtsi-hgi/cookie-monster.svg)](https://travis-ci.org/wtsi-hgi/cookie-monster)

# Cookie Monster

## Rules
### Changing rules on-the-fly
If ``RuleSource`` is being used by your ``ProcessorManager`` to attain the rules that are followed by ``Processor``
instances, it is possible to dynamically changes the rules used by the Cookie Monster for future jobs (jobs already 
running will continue to use the set of rules that they had when they were started).

The following code illustrates how a rule is registered. If appropriate, the code can be inserted into an existing rule 
file. Alternatively, it can be added to a new file in the rules directory, with a name matching the format: ``*.rule.py``.
Rule files can be put into subdirectories.
```python
from cookiemonster import register, Notification, Rule, RuleAction
from hgicommon.mixable import Priority 

def _matching_criteria(cookie: Cookie) -> bool:
    return "my_study" in cookie.path
        
def _action_generator(cookie: Cookie) -> RuleAction
    return RuleAction([Notification("everyone", cookie.path)], True)

_priority = Priority.MAX_PRIORITY

_rule = Rule(_matching_criteria, _action_generator, _priority)
register(rule)
```

To delete a pre-existing rule, delete the file containing it or remove the relevant call to ``register``. To modify a 
rule, simply change its code and it will be updated on save.


## Cookie Enrichments
### Changing enrichment loaders on-the-fly
Similarly to rules, the enrichment loaders, used to increase the knowledge of a cookie, can be changed during execution.
Files containing enrichment loaders must have a name matching the format: ``*.loader.py``.
```python
from cookiemonster import EnrichmentLoader, Cookie, Enrichment
from hgicommon.mixable import Priority 

def _can_enrich(cookie: Cookie) -> bool:
    return EnrichmentSource.MY_DATA_SOURCE in [enrichment.source for enrichment in cookie.enrichments]
    
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