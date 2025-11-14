Contributing to datashard
=========================

This section describes how to contribute to the datashard project.

Getting Started
---------------

Setting Up Development Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Fork the repository on GitHub
2. Clone your fork:

.. code-block:: bash

   git clone https://github.com/YOUR-USERNAME/datashard.git
   cd datashard

3. Create a virtual environment:

.. code-block:: bash

   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate

4. Install in development mode:

.. code-block:: bash

   pip install -e ".[dev]"

Development Workflow
--------------------

Creating a Feature Branch
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   git checkout -b feature/my-new-feature
   # or
   git checkout -b bugfix/issue-description

Code Style
^^^^^^^^^^

- Follow PEP 8 style guidelines
- Use type hints for all public functions
- Write docstrings in Google or NumPy format
- Keep functions reasonably small and focused
- Use meaningful variable and function names

Testing
^^^^^^^

All contributions must include appropriate tests:

.. code-block:: bash

   # Run all tests
   pytest

   # Run tests with coverage
   pytest --cov=datashard

   # Run specific test file
   pytest tests/test_iceberg.py

Code Review Process
-------------------

Submitting a Pull Request
^^^^^^^^^^^^^^^^^^^^^^^^^

1. Ensure all tests pass
2. Add or update documentation as needed
3. Update the changelog with your changes
4. Submit the pull request with a clear description

Review Guidelines
^^^^^^^^^^^^^^^^^

Pull requests will be reviewed for:

- Code quality and adherence to style
- Test coverage and correctness
- Performance implications
- Compatibility with existing functionality
- Documentation completeness

Documentation Standards
-----------------------

All contributions should include appropriate documentation:

- Function docstrings following Google or NumPy format
- User guides for new features
- API reference documentation
- Examples in the documentation

Example docstring format:

.. code-block:: python

   def my_function(param1: str, param2: int = 0) -> bool:
       """Short description of the function.

       Args:
           param1: Description of param1
           param2: Description of param2 with default value

       Returns:
           Description of return value

       Raises:
           ExceptionType: Description of when this exception is raised

       Example:
           >>> result = my_function("test", 5)
           >>> print(result)
           True
       """
       # Function implementation
       return True

Testing Requirements
--------------------

Unit Tests
^^^^^^^^^^

- All new functionality must have corresponding unit tests
- Aim for high test coverage (target >90%)
- Test edge cases and error conditions
- Use pytest for test organization

.. code-block:: python

   import pytest
   from datashard import create_table

   def test_table_creation():
       """Test that tables can be created successfully."""
       table = create_table("/tmp/test_table")
       assert table is not None
       assert table.table_path == "/tmp/test_table"

Integration Tests
^^^^^^^^^^^^^^^^^

- Test functionality across multiple modules
- Verify that different components work together correctly
- Test real-world usage scenarios

Running Tests
^^^^^^^^^^^^^

.. code-block:: bash

   # Run all tests
   pytest

   # Run with specific markers
   pytest -m "not slow"  # Skip slow tests

   # Run specific test file
   pytest tests/test_specific_module.py::test_specific_function

Submitting Changes
------------------

Before Submitting
^^^^^^^^^^^^^^^^^

1. Run all tests and ensure they pass
2. Check code formatting with linters:

.. code-block:: bash

   black src/datashard tests/
   ruff check src/datashard tests/

3. Update documentation if you've added new features
4. Add your changes to the changelog

Making the Pull Request
^^^^^^^^^^^^^^^^^^^^^^^

- Use a clear, descriptive title
- Explain what the changes do and why they're needed
- Include any relevant issue numbers
- Provide examples if showcasing new functionality

Community Guidelines
--------------------

This project follows the Python Community Code of Conduct. We are committed to providing a welcoming and inspiring community for all.

When contributing:

- Be respectful and inclusive
- Provide constructive feedback
- Help new contributors feel welcome
- Focus on technical merit and project goals

Getting Help
------------

If you need help with your contribution:

- Open an issue to discuss your proposed changes
- Reach out through the project's communication channels
- Look for the "help wanted" or "good first issue" labels for suitable starter tasks