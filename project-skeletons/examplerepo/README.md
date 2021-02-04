# examplerepo

Welcome to your new Dagster repository.

### Contents

| Name | Description |
|-|-|
| `examplerepo/` | A Python module that contains code for your Dagster repository |
| `examplerepo_tests/` | A Python module that contains tests for `examplerepo` |
| `README.md` | A description and guide for this code repository |
| `requirements.txt` | A list of Python package dependencies for this code repository |

## Getting up and running

1. Create a new Python environment and activate.

**Pyenv**
```bash
pyenv virtualenv X.Y.Z examplerepo
pyenv activate examplerepo
```

**Conda**
```bash
conda create --name examplerepo
conda activate examplerepo
```

2. Once you have activated your Python environment, install the Python dependencies.

```bash
pip install -r requirements.txt
```

## Local Development

1. Start the [Dagit process](https://docs.dagster.io/overview/dagit). This will start a Dagit web
server that, by default, is served on http://localhost:3000.

```bash
dagit -m examplerepo.repository
```

2. (Optional) If you want to enable Dagster
[Schedules](https://docs.dagster.io/overview/schedules-sensors/schedules) or
[Sensors](https://docs.dagster.io/overview/schedules-sensors/sensors) for your pipelines, start the
[Dagster Daemon process](https://docs.dagster.io/overview/daemon#main) **in a different shell or terminal**:

```bash
dagster-daemon run
```

## Local Testing

Tests can be found in `examplerepo_tests` and are run with the following command:

```bash
pytest examplerepo_tests
```

As you create Dagster solids and pipelines, add tests in `examplerepo_tests/` to check that your
code behaves as desired and does not break over time.

[For hints on how to write tests for solids and pipelines in Dagster,
[see our documentation tutorial on Testing](https://docs.dagster.io/tutorial/testable).
