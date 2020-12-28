import click
import yaml

CLI_HELP = """This CLI is used for generating Buildkite YAML.
"""


@click.group(help=CLI_HELP)
def cli():
    pass


@cli.command()
def pipeline():
    print(  # pylint: disable=print-call
        yaml.dump(
            {
                "env": {
                    "CI_NAME": "buildkite",
                    "CI_BUILD_NUMBER": "$BUILDKITE_BUILD_NUMBER",
                    "CI_BUILD_URL": "$BUILDKITE_BUILD_URL",
                    "CI_BRANCH": "$BUILDKITE_BRANCH",
                    "CI_PULL_REQUEST": "$BUILDKITE_PULL_REQUEST",
                },
                "steps": [],
            },
            default_flow_style=False,
        )
    )


def main():
    click_cli = click.CommandCollection(sources=[cli], help=CLI_HELP)
    click_cli()


if __name__ == "__main__":
    main()
