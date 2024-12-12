# data-engineering
learn data engineering, such as SQL on BigQuery and Python for big data using pandas.

### data science 100 for pandas

feat(main): add main function and refactor related functions

- Created a main function as the entry point.
- Refactored existing functions to improve clarity and maintainability.

refactor(config): extract __init__ logic into a dedicated helper function

- Moved initialization logic from Config.__init__ to a separate function.
- Improves code organization and testability.

refactor(config): utilize __post_init__ for initialization

- Modified Config class to use __post_init__ for initialization steps.
- Refactored related logic for better dataclass initialization flow.


fix(mypy): resolve mypy error by updating `re` imports and refactoring usage

- Replaced `from re import compile, search` with `import re` to address mypy type-check errors.
- Adjusted code to use `re.compile` and `re.search` directly.