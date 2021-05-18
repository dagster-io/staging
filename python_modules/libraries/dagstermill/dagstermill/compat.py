import packaging
import papermill
from papermill.exceptions import PapermillExecutionError

IS_PAPERMILL_2 = packaging.version.parse(papermill.__version__).major == 2



if IS_PAPERMILL_2:
    ExecutionError = PapermillExecutionError

else:
    from nbclient.exceptions import CellExecutionError

    ExecutionError = (PapermillExecutionError, CellExecutionError)
