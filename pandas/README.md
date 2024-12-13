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


chore: run isort to format imports

- Sort imports using isort to maintain a standardized import order


docs(config): clarify comments for config attributes

- Improve the clarity and readability of comments related to config attributes

refactor(config): revise __post_init__ and member methods

- Modify the implementation of __post_init__ for improved initialization logic
- Refactor member methods for better clarity and maintainability
- Convert methods to use @property decorators for consistency
- Implement functools.cached_property to optimize repeated lookups
- Adjust set naming conventions to follow the project’s style guidelines

docs(state): add return value docs for state_to_page_col_list and unify typing.List usage

- Document the return value of state_to_page_col_list
- Standardize usage of typing.List for type hints

refactor: add type annotations to create_col_list

- Add type annotations to arguments and return value of create_col_list for clearer code contracts


docs(main): update Main function docstring to follow Google style

- Rewrite the Main function docstring according to the Google docstring style guidelines

docs(preloader): rewrite Select_preloader function docstring

- Improve the docstring for Select_preloader to accurately describe the function’s purpose and usage






