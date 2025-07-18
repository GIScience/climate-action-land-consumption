# <img src="resources/info/icon.jpeg" width="5%"> Land Consumption

The Land Consumption Plugin created in partnership with WWF Österreich.

## Development setup

To run your plugin locally requires the following setup:

1. Set up the [infrastructure](https://gitlab.heigit.org/climate-action/infrastructure) locally in `devel` mode
2. Copy your [.env.base_template](.env.base_template) to `.env.base` and update it
3. Copy your [.env_template](.env_template) to `.env` and update it
4. Run `poetry run python land_consumption/plugin.py`

If you want to run your plugin through Docker, refer to
the [Plugin Showcase](https://gitlab.heigit.org/climate-action/plugins/plugin-showcase).

### Testing

We use [pytest](pytest.org) as testing engine.
Ensure all tests are passing on the unmodified repository by running `poetry run pytest`.

#### Coverage

To get a coverage report of how much of your code is run during testing, execute
`poetry run pytest --ignore test/test_plugin.py --cov`.
We ignore `test/test_plugin.py` when assessing coverage because the core tests run the whole plugin to be sure
everything successfully runs with a very basic configuration.
Yet, they don't actually test functionality and therefore artificially inflate the test coverage results.

To get a more detailed report including which lines in each file are **not** tested,
run `poetry run pytest --ignore test/test_plugin.py --cov --cov-report term-missing`

### Linting and formatting

It is important that the code created by the different plugin developers adheres to a certain standard.
We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting the code as part of our pre-commit hooks.
Please activate pre-commit by running `poetry run pre-commit install`.
It will now run automatically before each commit and apply fixes for a variety of lint errors to your code.
Note that we have increased the maximum number of characters per line to be 120 to make better use of large modern displays.
