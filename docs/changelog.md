# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.

## [0.6.0] - 2023-10-XX
### Added

- Add pipestat reader submodule to read DB results via FastAPI endpoints: `pipestat serve --config "config.yaml"`
- Add ability to create `SamplePipestatManager` and `ProjectSamplePipestatManager` which are sample/project specific PipestatManager objects.
- Add PipestatBoss wrapper class which holds `SamplePipestatManager` and `ProjectSamplePipestatManager` classes.
- Add `to_dict` methods to parsed schema object.

### Fixed
- Added path expansion when creating database url

### Changed
- modify `get_records` to return new structure
- Remove `get_orm` and replace with `get_model`
- Remove `get_table_name` function
- Refactor:
  - `sample_name` -> `record_identifier`  

## [0.5.1] - 2023-08-14
### Fixed

- fix schema_path issue when building html reports and obtaining schema from config file.

## [0.5.0] - 2023-08-08
### Added

- Add summarize function to generate static html results report.

## [0.4.1] - 2023-07-26

### Fix

- ensure Pipestat uses proper versions of Pydantic, Yacman.

## [0.4.0] - 2023-06-29

### Changed

- Remove attmap dependency
- Migrate to SQLModel for Object–relational mapping (ORM)
- Renamed `list_existing_results` to `list_results` and allow for returning a subset of results.
- Refactor: 
  - `namespace` -> `project_name`, 
  - `pipeline_id` -> `pipeline_name`, 
  - `record_identifier` -> `sample_name`

### Added

- Add 'init -g' for creating generic configuration file.
- Add ability to pass function to format reported results.

## [0.3.1] - 2022-08-18

### Fix

- database connection error

## [0.3.0] - 2021-10-30

### Added

- `select_distinct` for select distinct values from given table and column(s)

## [0.2.0] - 2021-10-25

### Added

- optional parameter for specify returning columns with `select_txt`
 
## [0.1.0] - 2021-06-24

**This update introduces some backwards-incompatible changes due to database interface redesign**

### Changed

- database interface type from a driver to an Object–relational mapping (ORM) approach

### Added

- results highlighting support
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
