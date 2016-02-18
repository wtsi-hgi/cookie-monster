[![Build Status](https://travis-ci.org/wtsi-hgi/cookie-monster.svg)](https://travis-ci.org/wtsi-hgi/cookie-monster)
[![codecov.io](https://codecov.io/github/wtsi-hgi/cookie-monster/coverage.svg?branch=master)](https://codecov.io/github/wtsi-hgi/cookie-monster?branch=master)

# Cookie Monster
"Cookie Monster" is a system that can source and aggregate information about data objects, with the ability to make 
decisions relating to these objects based upon the information known.


## Definitions
* The collection of all information known about a particular data object shall be referred to as a "Cookie".
* A subsystem that stores collection of Cookies shall be referred to as a "CookieJar".
* A subsystem that deals with the processing of a Cookie shall be referred to as a "Cookie Processor". 
* A "Rule" shall be defined as a matching criteria that is used against a Cookie, and an action, which is to be executed
if the criteria were satisfied.
* A process whereby more information is added to a Cookie shall be known as a "Cookie Enrichment".


## Components
### Cookie storage
At a minimum, a Cookie Monster installation uses a CookieJar to store Cookies. Each Cookie in the jar holds an the
identifier of the data object that the Cookie relates to ([beware of naming inconsistency](https://github.com/wtsi-hgi/cookie-monster/issues/16)).
A Cookie may also contain a number of Enrichments, each of which holds information about the data object, along with 
details about where and when this information was attained.

A CookieJar implementation (named "BiscuitTin"), which uses a CouchDB database, is supplied. It can be setup with:
```python
cookie_jar = BiscuitTin(couchdb_host, couchdb_database_name)
```

### Data retrievers
A Cookie Monster installation may use data retrievers, which get information about data objects that can be used to 
create/enrich Cookies in the CookieJar.
 
A retriever that periodically gets information about updates made to entities in an [iRODS database](https://irods.org/)
is shipped with the system. In order to use it, specific queries defined in [resources/specific-queries](resources/specific-queries)
must be installed on your iRODS server and a version of [baton](https://github.com/wtsi-npg/baton) must be installed.

### Cookie processing
A Cookie Monster installation may be setup with a Processor Manager, which uses Processors to examine Cookies after they 
have been enriched. A Processor first checks if a Cookie matches any predefined Rules, examined in order of priority. If 
a rule is matched, a Rule Action is executed: this action may specify a set of Notifications that should be broadcast
to any Notification Receivers, in addition to whether any more Rules should be ran. In the case where no Rule are 
matched, the Processor will check if the Cookie can be enriched further using an Enrichment Loader.

A simple implementation of a Processor Manager (named "BasicProcessorManager") is supplied. This can be constructed as
such:
```python
processor_manager = BasicProcessorManager(
    number_of_processors, cookie_jar, rules_source, enrichment_loader_source, notification_receivers_source)
```
Then setup to process Cookies as they are enriched in the CookieJar:
```python
cookie_jar.add_listener(processor_manager.process_any_cookies)
```

#### Rules
Rules have a matching criteria to which cookies are compared to determine if any action should be taken. If matched, 
the rule specifies an action for the cookie that can indicate that notification receivers should be informed and whether
further processing of the cookie is required. The order in which rules are applied is determined by their priority.

##### Changing rules on-the-fly
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

#### Cookie Enrichments
If all the rules have been applied against a cookie and none of them indicated in their action that no further
processing is required, cookie "enrichment" loaders can be used to load more information about a cookie.

##### Changing enrichment loaders on-the-fly
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

#### Notification Receivers
Rules can specify that a notification or set of notifications should be broadcast if a cookie matches the rule's
criteria; notification receivers receive these notifications.

##### Changing notification receivers on-the-fly
Notification receivers can also be changed on the fly in the same way as [rules](#rules) and 
[cookie enrichments](#cookie-enrichments). Files containing enrichment loaders must have a name matching the format:
``*.receiver.py``.
```python
from cookiemonster import Notification, NotificationReceiver
from hgicommon.data_source import register

def _receive(notification: Notification):
    if notification.about == "something_exciting":
        print(notification)
    
_notification_receiver = NotificationReceiver(_receive)
register(_notification_receiver)
```


### Data retrievers
A Cookie Monster installation may use data retrievers, which get updates about data objects that can be used to enrich 
(which will create if no previous information is known) related Cookies in the CookieJar.
 
A retriever that periodically gets information about updates made to entities in an [iRODS database](https://irods.org/)
is shipped with the system. In order to use it, specific queries defined in [resources/specific-queries](resources/specific-queries)
must be installed on your iRODS server and a version of [baton](https://github.com/wtsi-npg/baton) that supports 
specific queries ([such as that by wtsi-hgi](https://github.com/wtsi-hgi/baton/tree/feature/specificquery)) must be
installed. It can be setup as such:
```python
update_mapper = BatonUpdateMapper(baton_binaries_location)
database_connector = SQLAlchemyDatabaseConnector(retrieval_log_database)
retrieval_log_mapper = SQLAlchemyRetrievalLogMapper(database_connector)
retrieval_manager = PeriodicRetrievalManager(retrieval_period, update_mapper, retrieval_log_mapper)
```
Then linked to a CookieJar by:
```python
def put_update_in_cookie_jar(update_collection: UpdateCollection):
    for update in update_collection:
        enrichment = Enrichment("irods_update", datetime.now(), update.metadata)
        Thread(target=cookie_jar.enrich_cookie, args=(update.target, enrichment)).start()
retrieval_manager.add_listener(put_update_in_cookie_jar)
```


## HTTP API
A JSON-based HTTP API is provided to expose certain functionality as an outwardly facing interface, on a configurable 
port. Currently, the following endpoints are defined:

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
install all requirements for running the tests. Some tests use [Docker](https://www.docker.com) therefore a Docker
daemon must be running on the test machine, with the environment variables `DOCKER_TLS_VERIFY`, `DOCKER_HOST` and 
`DOCKER_CERT_PATH` set.
