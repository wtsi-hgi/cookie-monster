[![Build Status](https://travis-ci.org/wtsi-hgi/cookie-monster.svg)](https://travis-ci.org/wtsi-hgi/cookie-monster)

# Cookie Monster

## Rules
### Changing rules on-the-fly
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
    return RuleAction([Notification("everyone", cookie.path)], True)

_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _generate_action, _priority)
register(_rule)
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
from hgicommon.data_source import register

def _can_enrich(cookie: Cookie) -> bool:
    return "my_data_source" in [enrichment.source for enrichment in cookie.enrichments]
    
def _load_enrichment(cookie: Cookie) -> Enrichment:
    return my_data_source.load_more_information_about(cookie.path)

_priority = Priority.MAX_PRIORITY

_enrichment_loader = EnrichmentLoader(_can_enrich, _load_enrichment, _priority)
register(_enrichment_loader)
```

## HTTP API

A JSON-based HTTP API is provided to expose certain functionality as an
outwardly facing interface, on a configurable port. Currently, the
following endpoints are defined:

**`/queue`**
* `GET` Get the current status details of the "to process" queue,
  returning a JSON object with the following members: `queue_length`

**`/queue/reprocess`**
* `POST` Mark a file as requiring reprocessing, which will immediately
  return it (if necessary) to the "to process" queue. This method
  expects a JSON request body consisting of an object with a `path`
  member; returning the same.

Note that *all* requests must include `application/json` in their
`Accept` header.

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
