# Contributing to pg_lakehouse

Thank you for your interest in contributing to pg_lakehouse!

## Code of Conduct

This project adheres to the [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html). By participating, you are expected to uphold this code.

## How to Contribute

### Reporting Issues

- Search existing issues before creating a new one
- Use a clear and descriptive title
- Include steps to reproduce the issue
- Include your environment details (OS, PostgreSQL version, etc.)

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linters
5. Commit your changes with a clear message
6. Push to your fork
7. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/pg_lakehouse.git
cd pg_lakehouse

# Add upstream remote
git remote add upstream https://github.com/ORIGINAL_OWNER/pg_lakehouse.git

# Initialize submodules
git submodule update --init --recursive

# Install pre-commit hooks
pip install pre-commit
pre-commit install

# Start development environment
make docker-build
make docker-up
make docker-shell
```

### Code Style

- C/C++ code follows the style defined in `.clang-format`
- Run `make format` before committing
- Run `pre-commit run --all-files` to check all linters

### Commit Messages

Follow conventional commit format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Build/tooling changes

Example:
```
feat(iceberg): add basic scan support

Implement TableAmRoutine scan callbacks for reading Iceberg tables.

Closes #123
```

### Testing

- Write tests for new features
- Ensure all existing tests pass
- Run tests with `make test`

## License

By contributing to pg_lakehouse, you agree that your contributions will be licensed under the Apache License 2.0.
