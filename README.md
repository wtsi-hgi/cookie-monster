[![Build Status](https://travis-ci.org/wtsi-hgi/cookie-monster.svg)](https://travis-ci.org/wtsi-hgi/cookie-monster)
[![codecov.io](https://codecov.io/github/wtsi-hgi/cookie-monster/coverage.svg?branch=develop)](https://codecov.io/github/wtsi-hgi/cookie-monster?branch=develop)

# Cookie Monster
COOKIES! Om nom nom nom...


## Summary
1. Data retrievers can be setup to pull information into the system.
2. The information is aggregated in a knowledge base, grouped by its relation to a distinct entity.
3. When information becomes known about an entity, a production rule system is ran using rules that may have arbitrarily
complex preconditions that can be used to trigger arbitrarily complex productions.
4. Information about data objects can be easily enriched if it is determined that not enough information is known about
the object to process it.


## Key features
* DSL free.
* Python 3.5+.
* Simple to add production rules and methods of gathering more information on-the-fly.
* Available as a Docker image.


## Less documentation, more example
If you do not want to read about how the Cookie Monster system works and just want to look at an example of it in 
action, please see the [HGI Cookie Monster setup](https://github.com/wtsi-hgi/hgi-cookie-monster-setup/).


## Definitions
For better or for worse, naming within some parts of the system is Sesame Street themed...
* The collection of all information known about a particular data object is referred to as a "Cookie".
* The [subsystem that stores a collection of Cookies](#cookie-storage) is referred to as a "CookieJar".
* The [HTTP API](#http-api) is referred to as "Elmo".

The system is called "Cookie Monster" as its behaviour is [similar to that of the 
Cookie Monster character in Sesame Street](https://www.youtube.com/watch?v=I5e6ftNpGsU&feature=youtu.be&t=1m7s): it 
shovels in all of the cookies but only a few get digested/mashed into the hand puppet, with the rest falling back out.


## Components
### Cookie storage
At a minimum, a Cookie Monster installation comprises of a CookieJar that can store Cookies. It is essentially a
knowledge base that stores unstructured JSON data and a limited amount of associated metadata. Each Cookie in the jar 
holds an the identifier of the data object to which it relates. A Cookie may also contain a number of "enrichments", 
each of which holds information about the data object, along with details about where and when this information was 
attained.

A CookieJar implementation (named `BiscuitTin`), which uses a CouchDB database, is supplied. It can be setup with:
```python
cookie_jar = BiscuitTin(couchdb_host, couchdb_database_name)
```


### Cookie processing
A Cookie Monster installation can be setup with a Processor Manager, which uses Processors to examine Cookies after they 
have been enriched. Processors essentially implement a production rule system, where predefined rules are evaluated in 
order of priority. If a rule's precondition is matched, its action is triggered, which may be an arbitrary set of 
instructions. The action method's return value can be used to indicate whether any further rules should be processed 
with the cookie. In the case where no rules are matched/no rules indicate no further processing is required, the 
Processor will check if the Cookie can be enriched further using an Enrichment Loader and put any extra information into
the knowledge base.

A simple implementation of a Processor Manager (named `BasicProcessorManager`) is supplied. This can be constructed as
such:
```python
processor_manager = BasicProcessorManager(number_of_processors, cookie_jar, rules_source, enrichment_loader_source)
```
It can then be setup to process Cookies as they are enriched in the CookieJar:
```python
cookie_jar.add_listener(processor_manager.process_any_cookies)
```

#### Rules
Rules have a matching criteria (a precondition) to which Cookies are compared to determine if any action should be
taken. If matched, the rule's action is executed, which can be an arbitrary set of commands. The action method then 
returns whether further processing of the Cookie is required. The order in which rules are evaluated is determined by 
their priority.

##### Changing rules on-the-fly
If ``RuleSource`` is being used by your ``ProcessorManager`` to attain the rules that are evaluated by ``Processor``
instances, it is possible to dynamically changes the rules used by the Cookie Monster for future jobs (jobs already 
running will continue to use the set of rules that they had when they were started).

The following example illustrates how a rule is defined and registered. If appropriate, the code can be inserted into an 
existing rule file. Alternatively, it can be added to a new file in the rules directory, with a name matching the
format: ``*rule.py``. Rule files can be put into subdirectories. If the Python module does not compile (e.g. it
contains invalid syntax or uses a Python library that has not been installed), the module will be ignored.
```python
from cookiemonster.models import Cookie, Rule
from hgicommon.mixable import Priority
from hgicommon.data_source import register

MY_RULE_IDENTIFIER = "my_rule"

def _matches(cookie: Cookie, context: Context) -> bool:
    return "my_study" in cookie.path
        
def _action(cookie: Cookie, context: Context) -> bool:
    # <Interesting actions>
    return whether_any_more_rules_should_be_processed

_priority = Priority.MAX_PRIORITY

_rule = Rule(_matches, _generate_action, MY_RULE_IDENTIFIER, _priority)
register(_rule)
```

To delete a pre-existing rule, delete the file containing it or remove the relevant call to ``register``. To modify a 
rule, simply change its code and it will be updated in Cookie Monster when it is saved.

##### Examples
Please see the [rules used in the HGI Cookie Monster setup]
(https://github.com/wtsi-hgi/hgi-cookie-monster-setup/tree/master/hgicookiemonster/rules).


#### Cookie Enrichments
If all the rules have been evaluated and none of them defined in their action that no further processing of the Cookie
is required, cookie "enrichment loaders" can be used to load more information about a cookie.

##### Changing enrichment loaders on-the-fly
Similarly to [rules](#rules), the enrichment loaders can be changed during execution. Files containing enrichment
loaders must have a name matching the format: ``*loader.py``.
```python
from cookiemonster import EnrichmentLoader, Cookie, Enrichment
from hgicommon.mixable import Priority
from hgicommon.data_source import register

MY_ENRICHMENT_IDENTIFIER = "my_enrichment"

def _can_enrich(cookie: Cookie, context: Context) -> bool:
    return "my_data_source" in [enrichment.source for enrichment in cookie.enrichments]
    
def _load_enrichment(cookie: Cookie, context: Context) -> Enrichment:
    return my_data_source.load_more_information_about(cookie.path)

_priority = Priority.MAX_PRIORITY

_enrichment_loader = EnrichmentLoader(_can_enrich, _load_enrichment, MY_ENRICHMENT_IDENTIFIER, _priority)
register(_enrichment_loader)
```

##### Examples
Please see the [enrichment loaders used in the HGI Cookie Monster setup]
(https://github.com/wtsi-hgi/hgi-cookie-monster-setup/tree/master/hgicookiemonster/enrichment_loaders).


### Data retrievers
A Cookie Monster installation may use data retrievers, which get updates about data objects that can be used to enrich 
(which will create if no previous information is known) related Cookies in the CookieJar.
 
A retriever that periodically gets information about updates made to entities in an [iRODS database](https://irods.org)
is shipped with the system. In order to use it, the specific queries defined in
[resources/specific-queries](resources/specific-queries) must be installed on your iRODS server and a version of 
[baton](https://github.com/wtsi-npg/baton) above 0.16.3 must be installed. It can be
setup as such:
```python
update_mapper = BatonUpdateMapper(baton_binaries_location)
database_connector = SQLAlchemyDatabaseConnector(retrieval_log_database)
retrieval_log_mapper = SQLAlchemyRetrievalLogMapper(database_connector)
retrieval_manager = PeriodicRetrievalManager(retrieval_period, update_mapper, retrieval_log_mapper)
```
Then linked to a CookieJar by:
```python
executor = ThreadPoolExecutor(max_workers=NUMBER_OF_THREADS)

def put_updates_in_cookie_jar(update_collection: UpdateCollection):
    for update in update_collection:
        enrichment = Enrichment("irods_update", datetime.now(), update.metadata)
        executor.submit(timed_enrichment, update.target, enrichment)
retrieval_manager.add_listener(put_updates_in_cookie_jar)
```


### HTTP API
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

**`/cookiejar/<identifier>` (and `/cookiejar?identifier=<identifier>`)**
* `GET` Get a file and its enrichments from the metadata repository, by
  its identifier. (Note that the identifier must be percent encoded. If
  it begins with a slash, then the query string form of this endpoint
  *must* be used.)
* `DELETE` Delete a file and its enrichments from the metadata
  repository, by its identifier. (Note that the identifier must be
  percent encoded. If it begins with a slash, then the query string form
  of this endpoint *must* be used.)

**`/debug/threads`**
* `GET` Retrieve runtime state of all the current threads, for
  debugging.

Note that *all* requests must include `application/json` in their
`Accept` header.


## How to develop
### Testing
#### Locally
To run the tests, use ``./scripts/run-tests.sh`` from the project's root directory. This script will use ``pip`` to 
install all requirements for running the tests. Some tests use [Docker](https://www.docker.com) therefore a Docker
daemon must be running on the test machine, with the environment variables `DOCKER_TLS_VERIFY`, `DOCKER_HOST` and 
`DOCKER_CERT_PATH` set.
