from dagster import (
    AssetMaterialization,
    EventMetadata,
    ExpectationResult,
    In,
    Nothing,
    Out,
    Output,
    file_relative_path,
    graph,
    op,
)

MARKDOWN_EXAMPLE = "markdown_example.md"

raw_files = [
    "raw_file_users",
    "raw_file_groups",
    "raw_file_events",
    "raw_file_friends",
    "raw_file_pages",
    "raw_file_fans",
    "raw_file_event_admins",
    "raw_file_group_admins",
]


def create_raw_file_solid(name):
    @op(
        name=name,
        description=f"Inject raw file for input to table {name} and do expectation on output",
    )
    def raw_file_solid():
        yield AssetMaterialization(
            asset_key="table_info",
            metadata={"table_path": EventMetadata.path("/path/to/{}.raw".format(name))},
        )
        yield ExpectationResult(
            success=True,
            label="output_table_exists",
            description=f"Checked {name} exists",
        )
        yield Output(name)

    return raw_file_solid


raw_tables = [
    "raw_users",
    "raw_groups",
    "raw_events",
    "raw_friends",
    "raw_pages",
    "raw_fans",
    "raw_event_admins",
    "raw_group_admins",
]


def create_raw_file_solids():
    return list(map(create_raw_file_solid, raw_files))


def input_name_for_raw_file(raw_file):
    return raw_file + "_ready"


@op(
    ins={"start": In(dagster_type=Nothing)},
    out=Out(dagster_type=Nothing),
    description="Load a bunch of raw tables from corresponding files",
)
def many_table_materializations():
    with open(file_relative_path(__file__, MARKDOWN_EXAMPLE), "r") as f:
        md_str = f.read()
        for table in raw_tables:
            yield AssetMaterialization(
                asset_key="table_info",
                metadata={
                    "table_name": table,
                    "table_path": EventMetadata.path(f"/path/to/{table}"),
                    "table_data": {"name": table},
                    "table_name_big": EventMetadata.url(f"https://bigty.pe/{table}"),
                    "table_blurb": EventMetadata.md(md_str),
                    "big_int": 29119888133298982934829348,
                    "float_nan": float("nan"),
                },
            )


@op(
    ins={"start": In(dagster_type=Nothing)},
    out=Out(dagster_type=Nothing),
    description="This simulates a solid that would wrap something like dbt, "
    "where it emits a bunch of tables and then say an expectation on each table, "
    "all in one solid",
)
def many_materializations_and_passing_expectations():
    tables = [
        "users",
        "groups",
        "events",
        "friends",
        "pages",
        "fans",
        "event_admins",
        "group_admins",
    ]

    for table in tables:
        yield AssetMaterialization(
            asset_key="table_info",
            metadata={
                "table_path": EventMetadata.path(f"/path/to/{table}.raw"),
            },
        )
        yield ExpectationResult(
            success=True,
            label=f"{table}.row_count",
            description=f"Row count passed for {table}",
        )


@op(
    ins={"start": In(dagster_type=Nothing)},
    description="A solid that just does a couple inline expectations, one of which fails",
)
def check_users_and_groups_one_fails_one_succeeds():
    yield ExpectationResult(
        success=True,
        label="user_expectations",
        description="Battery of expectations for user",
        metadata={
            "table_summary": {
                "columns": {
                    "name": {"nulls": 0, "empty": 0, "values": 123, "average_length": 3.394893},
                    "time_created": {"nulls": 1, "empty": 2, "values": 120, "average": 1231283},
                }
            },
        },
    )

    yield ExpectationResult(
        success=False,
        label="groups_expectations",
        description="Battery of expectations for groups",
        metadata={
            "table_summary": {
                "columns": {
                    "name": {"nulls": 1, "empty": 0, "values": 122, "average_length": 3.394893},
                    "time_created": {"nulls": 1, "empty": 2, "values": 120, "average": 1231283},
                }
            }
        },
    )


@op(
    ins={"start": In(dagster_type=Nothing)},
    description="A solid that just does a couple inline expectations",
)
def check_admins_both_succeed():
    yield ExpectationResult(success=True, label="Group admins check out")
    yield ExpectationResult(success=True, label="Event admins check out")


@graph(
    description=(
        "Demo pipeline that yields AssetMaterializations and ExpectationResults, along with the "
        "various forms of metadata that can be attached to them."
    )
)
def many_events_graph():
    raw_files_solids = [raw_file_solid() for raw_file_solid in create_raw_file_solids()]

    mtm = many_table_materializations(raw_files_solids)
    mmape = many_materializations_and_passing_expectations(mtm)
    check_users_and_groups_one_fails_one_succeeds(mmape)
    check_admins_both_succeed(mmape)


many_events_job = many_events_graph.to_job(name="many_events_job")
