[![Build Status](https://travis-ci.org/wtsi-hgi/cookie-monster.svg)](https://travis-ci.org/wtsi-hgi/cookie-monster)

# Cookie Monster

## Changing Rules Whilst Running
If ``RuleSource`` is being used by your ``ProcessorManager`` to get the rules that are followed by ``Processor``
instances, it is possible to dynamically changes the rules used by the Cookie Monster for future jobs (jobs already 
running will continue to use the set of rules that they had when they were started).

The following code illustrates how a rule is registered. If appropriate, the code can be inserted into an existing rule 
file. Alternatively, it can be added to a new file in the rules directory, with a name matching the format 
``*.rule.py``. Rule files can be put into subdirectories.
```python
from cookiemonster import register, Notification, Rule, RuleAction
from hgicommon.mixable import Priority 


def _matching_criteria(cookie: Cookie) -> bool:
    return "my_study" in cookie.path
    
    
def _action_generator(cookie: Cookie) -> RuleAction
    return RuleAction([Notification("everyone", cookie.path)], True)


_priority = Priority.MAX_PRIORITY


register(Rule(_matching_criteria, _action_generator, _priority))
```

To delete a pre-existing rule, delete the file containing it or remove the relevant call to ``register``. To modify a 
rule, simply change its code and it will be updated on save.


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