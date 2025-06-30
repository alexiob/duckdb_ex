# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2025-06-30

### Added

- Initial release of DuckdbEx
- Elixir NIF wrapper for DuckDB database
- Support for database connections, queries, and prepared statements
- CSV import functionality
- Comprehensive documentation and guides
- NIF management system with smart build optimization
- Custom Mix tasks for NIF management (`nif.download`, `nif.rebuild`, `nif.clean`)

### Changed

- Optimized NIF build process to avoid unnecessary rebuilds
- Added environment variable controls for NIF build behavior

### Fixed

- NIF rebuild issue on every compile, iex, or test run
- Improved error handling and validation in NIF operations

## [0.1.0] - Initial Release

### Features

- Basic DuckDB functionality through Elixir NIF
- Connection management
- Query execution
- Prepared statements
- CSV data import
- Documentation and examples
