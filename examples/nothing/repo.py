from dagster import nothing_input, nothing_output, pipeline, repository, solid


def get_database_connection():
    class Database:
        def execute(self, query):
            pass

    return Database()


# start_repo_marker_0
@solid(output_defs=[nothing_output()])
def create_table_1(_):
    get_database_connection().execute("create table_1 as select * from some_source_table")


@solid(input_defs=[nothing_input()])
def create_table_2(_):
    get_database_connection().execute("create table_2 as select * from table_1")


@pipeline
def my_pipeline():
    create_table_2(create_table_1())


# end_repo_marker_0


@repository
def nothing_example_repository():
    return [my_pipeline]
