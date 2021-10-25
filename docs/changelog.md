# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.

## [0.2.0] - 2021-10-25

### Added

- optional parameter for specify returning columns with `select_txt`
 
## [0.1.0] - 2021-06-24

**This update introduces some backwards-incompatible changes due to database interface redesign**

### Changed

- database interface type from a driver to an Object–relational mapping (ORM) approach

### Added

- results highligting support
- database column parametrizing from the results schema
- static typing
- possibility to initialize the `PipestatManager` object (or use the `pipestat status` CLI) with no results schema defined for pipeline status management even when backed by a database; [Issue #1](https://github.com/pepkit/pipestat/issues/1)

## [0.0.4] - 2021-04-02

### Added

- config validation
- typing in code

## [0.0.3] - 2021-03-12

### Added

- possibility to initialize the `PipestatManager` object (or use the `pipestat status` CLI) with no results schema defined for pipeline status management; [Issue #1](https://github.com/pepkit/pipestat/issues/1)

## [0.0.2] - 2021-02-22

### Added

- initial package release
