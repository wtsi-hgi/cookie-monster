# Change Log
## 1.1.1 - 2016-08-09
### Added
- Wrapped all high-level CouchDB calls in a persistent-retry decorator
  to deal with the occasional DB instability that we can't otherwise
  cater for. This should be disabled while debugging.
## 1.1.0 (Cognizant Custard Cream) - 2016-07-29
### Added
- Logging of number of threads waiting to a get a Cookie to process.
- More detailed logging of database operations and CouchDB response times.
- Ability to get more than one cookie for processing at a time.
- After a rule is matched and its production is executed, the
  corresponding cookie is enriched with a `RuleApplicationLog` from the
  `RULE_APPLICATION` source
  ([42](https://github.com/wtsi-hgi/python-baton-wrapper)).
- Thread debug dump HTTP API endpoint.

### Changed
- Identifiers for rules and enrichment loaders are now mandatory.
- Cookie enrichments are now stored in an `EnrichmentCollection`. Helper
  methods for searching enrichments have been removed from the `Cookie`
  class and moved to the collection
  ([43](https://github.com/wtsi-hgi/cookie-monster/issues/43)).
- All CouchDB requests are now subject to graceful retrying in the event
  of an unknown (e.g., server/network) failure; by default, it won't
  give up ([47](https://github.com/wtsi-hgi/cookie-monster/issues/47)).

### Fixed
- Document locks in CouchDB batching interface was not thread-safe
  ([45](https://github.com/wtsi-hgi/cookie-monster/issues/45)).

## 1.0.0 (Benevolent Bourbon) - 2016-05-16
### Added
- Query string interface for fetch/delete HTTP API (as well as URL).
- Enrichment diff convenience method.
- Imported Dockerfile from legacy https://github.com/wtsi-hgi/docker-cookie-monster project.
- Legalese boilerplate, in line with WTSI policy.
- HTTP connection pool configuration (for downstream applications).

### Removed
- Notification listeners.

### Fixed
- InfluxDB timestamp rounding.
- Rate limiting causing deadlock in CouchDB interface.
- CouchDB interface blocking.

## 0.5.0 (Adaptable Amaretti) - 2016-03-21
### Added
- First stable release.
