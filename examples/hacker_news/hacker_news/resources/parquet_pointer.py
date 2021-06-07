from collections import namedtuple


class ParquetPointer(namedtuple("_ParquetPointer", "path schema")):
    """
    Class that contains a path to a parquet file (either local or in external storage), as well as
    the schema of the parquet object stored there.
    """
