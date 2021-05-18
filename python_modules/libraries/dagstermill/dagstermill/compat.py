from papermill.exceptions import PapermillExecutionError

try:
    # papermill 1.x
    from nbclient.exceptions import CellExecutionError

    ExecutionError = (PapermillExecutionError, CellExecutionError)
except ImportError:
    # papermill 2.x

    ExecutionError = PapermillExecutionError
