# Project Template: Single-Repo

## Contents

| Name | Description |
|-|-|
| `my_project` | A Python module that contains Dagster code |
| `my_project_tests` | A Python module that contains tests for `my_project` code |
| `README.md` | A project README |

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
dagit -m my_project.repository -d dev
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

1. Build the Docker image.

```bash
docker build . -t dagster-my_project
```

2. Run the Docker image.

```bash
docker -p 4000:4000 run dagster-my_project:latest
```

3. In another terminal, run Dagit.

```bash
dagit --grpc-host=localhost --grpc-port=4000
```
