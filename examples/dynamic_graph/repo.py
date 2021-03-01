# start_dynamic
import os
from time import sleep
from typing import List, NamedTuple

from dagster import Field, RetryRequested, pipeline, repository, solid, usable_as_dagster_type
from dagster.core.definitions.output import OutputDefinition
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from dagster.utils import file_relative_path


@usable_as_dagster_type
class DirectoryReport(NamedTuple):
    total_size: int


@usable_as_dagster_type
class FileReport(NamedTuple):
    size: int


@solid(
    config_schema={"path": Field(str, default_value=file_relative_path(__file__, "sample"))},
)
def process_directory(_) -> DirectoryReport:
    # ...
    return DirectoryReport(0)


@solid
def process_files(_, files: List[str]) -> List[FileReport]:
    file_reports = files
    # ...
    return file_reports


@solid(
    config_schema={"path": Field(str, default_value=file_relative_path(__file__, "sample"))},
)
def ls_directory(_) -> List[str]:
    return []


@solid(
    config_schema={"path": Field(str, default_value=file_relative_path(__file__, "sample"))},
    output_defs=[
        OutputDefinition(List[str], "files_1"),
        OutputDefinition(List[str], "files_2"),
        OutputDefinition(List[str], "files_3"),
        OutputDefinition(List[str], "files_4"),
    ],
)
def ls_directory_chunked(_) -> List[str]:
    return []


@solid
def collect_reports(_, results: List[FileReport]) -> DirectoryReport:
    return DirectoryReport(sum([r.size for r in results]))


@solid
def collect_reports_chunked(_, results: List[List[FileReport]]) -> DirectoryReport:
    return None


RETRIED = False


@solid
def process_file(_, path: str) -> FileReport:
    sleep(0.15)

    # smoke and mirrors
    global RETRIED
    if "file_c" in path and not RETRIED:
        RETRIED = True
        raise RetryRequested()

    # simple example of calculating size
    return FileReport(os.path.getsize(path))


#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
###################################################################################################
# DEMO ZONE
###################################################################################################


@solid(
    config_schema={
        "path": Field(
            str,
            default_value=file_relative_path(__file__, "sample"),
        )
    },
    output_defs=[DynamicOutputDefinition(str)],
)
def files_in_directory(context):
    path = context.solid_config["path"]
    dirname, _, filenames = next(os.walk(path))
    for file in filenames:
        yield DynamicOutput(
            value=os.path.join(dirname, file),
            # create a mapping key from the file name
            mapping_key=file.replace(".", "_").replace("-", "_"),
        )


@pipeline
def process_directory_dynamic():
    dynamic_directories = files_in_directory()
    reports = dynamic_directories.map(process_file)
    collect_reports(reports.collect())


###################################################################################################
#
###################################################################################################
#
#
#
#
#
#
#


@pipeline
def process_directory_simple():
    files = ls_directory()
    results = process_files(files)
    collect_reports(results)


@pipeline
def process_directory_fixed():
    f1, f2, f3, f4 = ls_directory_chunked()
    collect_reports_chunked(
        [process_files(f1), process_files(f2), process_files(f3), process_files(f4)]
    )


@pipeline
def directory_report():
    process_directory()


@repository
def dynamic_graph_repository():
    return [
        process_directory_fixed,
        process_directory_dynamic,
        directory_report,
        process_directory_simple,
    ]


# end_dynamic
