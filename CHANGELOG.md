# Change Log
## [Unreleased]
### Added
- Logging of number of threads waiting to a get a Cookie to process.
- Ability to get more than one cookie for processing at a time.
- After a rule is matched and its production is executed, the corresponding cookie is enriched with a
`RuleApplicationLog` from the `RULE_APPLICATION` source ([42](https://github.com/wtsi-hgi/python-baton-wrapper)).

### Changed
- Identifiers for rules and enrichment loaders are no mandatory.

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
