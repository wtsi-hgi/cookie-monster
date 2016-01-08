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
Similarly to [rules](#rules), the enrichment loaders can be changed during execution. Files containing enrichment
loaders must have a name matching the format: ``*.loader.py``.
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


### Notification Receivers
Rules can specify that a notification or set of notifications should be broadcast if a cookie matches the rule's
criteria; notification receivers receive these notifications. They can then determine what action should be taken.

#### Changing notification receivers on-the-fly
Notification receivers can also be changed on the fly in the same way [rules](#rules) and 
[cookie enrichments](#cookie-enrichments). Files containing enrichment loaders must have a name matching the format:
``*.notification_receivers.py``.
```python
from cookiemonster import Notification, NotificationReceiver
from hgicommon.data_source import register

def _retrieve(notification: Notification) -> bool:
    if notification.about == "something_exciting":
        print(notification)
    
_notification_receiver = NotificationReceiver(_retrieve)
register(_notification_receiver)
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