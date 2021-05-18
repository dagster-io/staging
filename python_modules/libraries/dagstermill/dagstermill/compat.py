try:
    # papermill 1.x
    from nbclient.exceptions import CellExecutionError

    ExecutionError = CellExecutionError
except ImportError:
    # papermill 2.x
    from papermill.exceptions import PapermillExecutionError

    ExecutionError = PapermillExecutionError
