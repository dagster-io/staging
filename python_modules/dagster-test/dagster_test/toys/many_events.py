from dagster import (
    AssetMaterialization,
    EventMetadataEntry,
    ExpectationResult,
    InputDefinition,
    Nothing,
    Output,
    OutputDefinition,
    file_relative_path,
    pipeline,
    solid,
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
    def do_expectation(_context, _value):
        return ExpectationResult(
            success=True,
            label="output_table_exists",
            description="Checked {name} exists".format(name=name),
        )

    @solid(
        name=name,
        description="Inject raw file for input to table {} and do expectation on output".format(
            name
        ),
    )
    def raw_file_solid(_context):
        yield AssetMaterialization(
            asset_key="table_info",
            metadata_entries=[
                EventMetadataEntry.path(label="table_path", path="/path/to/{}.raw".format(name))
            ],
        )
        yield do_expectation(_context, name)
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


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    output_defs=[OutputDefinition(Nothing)],
    description="Load a bunch of raw tables from corresponding files",
)
def many_table_materializations(_context):
    with open(file_relative_path(__file__, MARKDOWN_EXAMPLE), "r") as f:
        md_str = f.read()
        for table in raw_tables:
            yield AssetMaterialization(
                asset_key="table_info",
                metadata_entries=[
                    EventMetadataEntry.text(text=table, label="table_name"),
                    EventMetadataEntry.fspath(path="/path/to/{}".format(table), label="table_path"),
                    EventMetadataEntry.json(data={"name": table}, label="table_data", inline=True),
                    EventMetadataEntry.url(
                        url="https://bigty.pe/{}".format(table), label="table_name_big"
                    ),
                    EventMetadataEntry.md(md_str=md_str, label="table_blurb"),
                    EventMetadataEntry.html(
                        html="""
                        <svg class="chart" width="420" height="100" aria-labelledby="title desc" role="img">
                            <title id="title">A bar chart showing information</title>
                            <desc id="desc">4 apples; 8 bananas; 15 kiwis; 16 oranges; 23 lemons</desc>
                            <g class="bar">
                                <rect width="40" height="19"></rect>
                                <text x="45" y="9.5" dy=".35em">4 apples</text>
                            </g>
                            <g class="bar">
                                <rect width="80" height="19" y="20"></rect>
                                <text x="85" y="28" dy=".35em">8 bananas</text>
                            </g>
                            <g class="bar">
                                <rect width="150" height="19" y="40"></rect>
                                <text x="150" y="48" dy=".35em">15 kiwis</text>
                            </g>
                            <g class="bar">
                                <rect width="160" height="19" y="60"></rect>
                                <text x="161" y="68" dy=".35em">16 oranges</text>
                            </g>
                            <g class="bar">
                                <rect width="230" height="19" y="80"></rect>
                                <text x="235" y="88" dy=".35em">23 lemons</text>
                            </g>
                        </svg>
                        """,
                        label="svg graph",
                    ),
                    EventMetadataEntry.html(
                        html="""
                        <img src="data:image/jpeg;base64,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"
                        width="400" height="225"/>
                        """,
                        label="image",
                        inline=True,
                    ),
                ],
            )


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    output_defs=[OutputDefinition(Nothing)],
    description="This simulates a solid that would wrap something like dbt, "
    "where it emits a bunch of tables and then say an expectation on each table, "
    "all in one solid",
)
def many_materializations_and_passing_expectations(_context):
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
            metadata_entries=[
                EventMetadataEntry.path(label="table_path", path="/path/to/{}.raw".format(table))
            ],
        )
        yield ExpectationResult(
            success=True,
            label="{table}.row_count".format(table=table),
            description="Row count passed for {table}".format(table=table),
        )


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    output_defs=[],
    description="A solid that just does a couple inline expectations, one of which fails",
)
def check_users_and_groups_one_fails_one_succeeds(_context):
    yield ExpectationResult(
        success=True,
        label="user_expectations",
        description="Battery of expectations for user",
        metadata_entries=[
            EventMetadataEntry.json(
                label="table_summary",
                data={
                    "columns": {
                        "name": {"nulls": 0, "empty": 0, "values": 123, "average_length": 3.394893},
                        "time_created": {"nulls": 1, "empty": 2, "values": 120, "average": 1231283},
                    }
                },
            )
        ],
    )

    yield ExpectationResult(
        success=False,
        label="groups_expectations",
        description="Battery of expectations for groups",
        metadata_entries=[
            EventMetadataEntry.json(
                label="table_summary",
                data={
                    "columns": {
                        "name": {"nulls": 1, "empty": 0, "values": 122, "average_length": 3.394893},
                        "time_created": {"nulls": 1, "empty": 2, "values": 120, "average": 1231283},
                    }
                },
            )
        ],
    )


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    output_defs=[],
    description="A solid that just does a couple inline expectations",
)
def check_admins_both_succeed(_context):
    yield ExpectationResult(success=True, label="Group admins check out")
    yield ExpectationResult(success=True, label="Event admins check out")


@pipeline(
    description=(
        "Demo pipeline that yields AssetMaterializations and ExpectationResults, along with the "
        "various forms of metadata that can be attached to them."
    )
)
def many_events():
    raw_files_solids = [raw_file_solid() for raw_file_solid in create_raw_file_solids()]

    mtm = many_table_materializations(raw_files_solids)
    mmape = many_materializations_and_passing_expectations(mtm)
    check_users_and_groups_one_fails_one_succeeds(mmape)
    check_admins_both_succeed(mmape)
