from typing import NamedTuple


class ParquetPointer(NamedTuple):
    """
    Class that contains a path to a parquet file (either local or in external storage), as well as
    the schema of the parquet object stored there.
    """

    path: str
    schema: str
