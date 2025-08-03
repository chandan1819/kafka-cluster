# Contributing to Local Kafka Manager

Thank you for your interest in contributing to Local Kafka Manager! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Docker 20.0+
- Docker Compose 2.0+
- Git

### Development Setup

1. **Fork and clone the repository**:
   ```bash
   git clone https://github.com/chandan1819/kafka-cluster.git
   cd kafka-cluster
   ```

2. **Run the installation script**:
   ```bash
   ./install.sh
   ```

3. **Install development dependencies**:
   ```bash
   source venv/bin/activate
   pip install -e ".[dev]"
   ```

4. **Start the development environment**:
   ```bash
   ./start.sh
   ```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest

# Run specific test categories
pytest -m unit          # Unit tests only
pytest -m integration   # Integration tests only
pytest -m performance   # Performance tests only

# Run with coverage
pytest --cov=src --cov-report=html

# Run end-to-end tests
pytest tests/test_end_to_end_integration.py -v
```

### Test Structure

- **Unit Tests**: `tests/test_*.py` - Test individual components
- **Integration Tests**: `tests/test_integration.py` - Test service interactions
- **End-to-End Tests**: `tests/test_end_to_end_integration.py` - Full workflow tests
- **Performance Tests**: `tests/test_performance.py` - Load and performance tests

### Writing Tests

- Use pytest fixtures from `tests/conftest.py`
- Mock external dependencies (Docker, Kafka)
- Follow the existing test patterns
- Add both positive and negative test cases
- Include performance assertions where relevant

## 🏗️ Architecture

### Project Structure

```
├── src/                    # Source code
│   ├── api/               # FastAPI routes and endpoints
│   ├── models/            # Pydantic data models
│   ├── services/          # Business logic services
│   ├── utils/             # Utility functions and helpers
│   ├── config/            # Configuration management
│   ├── middleware/        # FastAPI middleware
│   └── main.py           # Application entry point
├── tests/                 # Comprehensive test suite
├── docs/                  # Additional documentation
├── scripts/               # Utility scripts
├── docker-compose.yml     # Docker services definition
└── requirements.txt       # Python dependencies
```

### Key Components

1. **API Layer** (`src/api/`): FastAPI routes and request handling
2. **Service Layer** (`src/services/`): Business logic and Kafka operations
3. **Model Layer** (`src/models/`): Data validation and serialization
4. **Utils Layer** (`src/utils/`): Logging, metrics, retry logic

## 📝 Code Style

### Python Code Standards

- **Formatter**: Black (line length: 88)
- **Import Sorting**: isort
- **Type Checking**: mypy
- **Linting**: flake8
- **Docstrings**: Google style

### Running Code Quality Checks

```bash
# Format code
black src/ tests/
isort src/ tests/

# Type checking
mypy src/

# Linting
flake8 src/ tests/

# Run all checks
./scripts/check-code-quality.sh
```

### Commit Message Format

Use conventional commits:

```
type(scope): description

feat(api): add new endpoint for consumer group management
fix(cluster): resolve startup timeout issue
docs(readme): update installation instructions
test(integration): add end-to-end workflow tests
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`

## 🔄 Development Workflow

### Making Changes

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write code following the style guidelines
   - Add tests for new functionality
   - Update documentation as needed

3. **Test your changes**:
   ```bash
   pytest
   ./scripts/check-code-quality.sh
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat(scope): your descriptive message"
   ```

5. **Push and create a pull request**:
   ```bash
   git push origin feature/your-feature-name
   ```

### Pull Request Guidelines

- **Title**: Clear, descriptive title following conventional commits
- **Description**: Explain what changes were made and why
- **Testing**: Describe how the changes were tested
- **Documentation**: Update relevant documentation
- **Breaking Changes**: Clearly mark any breaking changes

## 🐛 Bug Reports

When reporting bugs, please include:

- **Environment**: OS, Python version, Docker version
- **Steps to Reproduce**: Clear, step-by-step instructions
- **Expected Behavior**: What should happen
- **Actual Behavior**: What actually happens
- **Logs**: Relevant log output from `logs/api.log` or `docker compose logs`
- **Configuration**: Any custom configuration used

## 💡 Feature Requests

For new features, please provide:

- **Use Case**: Why is this feature needed?
- **Proposed Solution**: How should it work?
- **Alternatives**: Other solutions considered
- **Implementation Ideas**: Technical approach (if any)

## 📚 Documentation

### Types of Documentation

- **README.md**: Main project documentation
- **SETUP.md**: Detailed setup instructions
- **API Documentation**: Auto-generated from FastAPI
- **Code Comments**: Inline documentation for complex logic
- **Docstrings**: Function and class documentation

### Documentation Standards

- Use clear, concise language
- Include code examples where helpful
- Keep documentation up-to-date with code changes
- Use proper markdown formatting
- Include screenshots for UI-related features

## 🚀 Release Process

### Versioning

We use [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Release Checklist

- [ ] All tests pass
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped in relevant files
- [ ] Git tag created
- [ ] Release notes written

## 🤝 Community

### Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Documentation**: Check README.md and SETUP.md first

### Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Follow the project's technical standards

## 🎯 Areas for Contribution

### High Priority

- **Performance Optimization**: Improve startup times and throughput
- **Error Handling**: Better error messages and recovery
- **Monitoring**: Enhanced metrics and observability
- **Documentation**: More examples and tutorials

### Medium Priority

- **UI Enhancements**: Improve the web interface
- **Configuration**: More customization options
- **Testing**: Increase test coverage
- **Platform Support**: Windows compatibility improvements

### Good First Issues

Look for issues labeled `good-first-issue` or `help-wanted` in the GitHub repository.

## 📞 Contact

- **Maintainer**: [Your Name]
- **GitHub**: [@chandan1819](https://github.com/chandan1819)
- **Issues**: [GitHub Issues](https://github.com/chandan1819/kafka-cluster/issues)

Thank you for contributing to Local Kafka Manager! 🎉