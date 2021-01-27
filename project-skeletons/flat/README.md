# Project Template: Flat

## Contents

| Name | Description |
|-|-|
| `my_project` | A Python module that contains Dagster code |
| `my_project_tests` | A Python module that contains tests for `my_project` code |
| `README.md` | A project README |
| `workspace.yaml` | A list of Dagster repository locations. See the [Overview on Workspaces](https://docs.dagster.io/overview/repositories-workspaces/workspaces) for details. |

## Getting up and running

Make sure that you have `dagster` and `dagit` installed in your python environment.

```bash
pip install dagster dagit
```

### Local Development

1. Start the Dagster daemon process:

```bash
dagster-daemon run
```

2. **In a different terminal**, start the Dagit process:

```bash
dagit -w workspace.yaml -d dev
```

### Local Testing

Make sure that you have `pytest` installed in your python environment.

```bash
pip install pytest
```

Tests can be found in `my_project_tests` and are run with the following command:

```bash
pytest my_project_tests
```

### Deploying to Production

TODO
