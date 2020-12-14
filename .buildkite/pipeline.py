# This file is temporary to decouple D5777 from changing the Buildkite web UI. Will be removed
# in a follow-up diff
import subprocess


def main():
    res = subprocess.check_call(
        "python3 -m pip install --user -e .buildkite/dagster-buildkite &> /dev/null", shell=True
    )
    assert res == 0

<<<<<<< HEAD
    buildkite_yaml = subprocess.check_output("~/.local/bin/dagster-buildkite", shell=True)
    print(buildkite_yaml)  # pylint: disable=print-call
=======

def pylint_steps():
    base_paths = [".buildkite", "bin", "docs/next/src"]
    base_paths_ext = ['"%s/**.py"' % p for p in base_paths]

    return [
        StepBuilder("pylint misc")
        .run(
            # Deps needed to pylint docs
            """pip install \
                -e python_modules/dagster \
                -e python_modules/dagit \
                -e python_modules/automation \
                -e python_modules/libraries/dagstermill \
                -e python_modules/libraries/dagster-celery \
                -e python_modules/libraries/dagster-dask \
                -e examples/legacy_examples
            """,
            "pylint -j 0 `git ls-files %s` --rcfile=.pylintrc" % " ".join(base_paths_ext),
        )
        .on_integration_image(SupportedPython.V3_7)
        .build()
    ]


def next_docs_build_tests():
    return [
        StepBuilder("next docs build tests")
        .run(
            "pip install -e python_modules/automation",
            "pip install -r docs-requirements.txt -qqq",
            "pip install -r python_modules/dagster/dev-requirements.txt -qqq",
            "cd docs",
            "make NODE_ENV=production VERSION=master full_docs_build",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
        StepBuilder("next docs tests")
        .run(
            "pip install -e python_modules/automation",
            "pip install -r docs-requirements.txt -qqq",
            "pip install -r python_modules/dagster/dev-requirements.txt -qqq",
            "cd docs",
            "make buildnext",
            "cd next",
            "yarn test",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
        StepBuilder("documentation coverage")
        .run(
            "make install_dev_python_modules",
            "pip install -e python_modules/automation",
            "pip install -r docs-requirements.txt -qqq",
            "cd docs",
            "make updateindex",
            "pytest -vv test_doc_build.py",
            "git diff --exit-code",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]


def version_equality_checks(version=SupportedPython.V3_7):
    return [
        StepBuilder("version equality checks for libraries")
        .on_integration_image(version)
        .run("pip install -e python_modules/automation", "dagster-release version")
        .build()
    ]


def dagit_steps():
    return [
        StepBuilder("dagit webapp tests")
        .run(
            "pip install -r python_modules/dagster/dev-requirements.txt -qqq",
            "pip install -e python_modules/dagster -qqq",
            "pip install -e python_modules/dagster-graphql -qqq",
            "pip install -e python_modules/libraries/dagster-cron -qqq",
            "pip install -e python_modules/libraries/dagster-slack -qqq",
            "pip install -e python_modules/dagit -qqq",
            "pip install -e examples/legacy_examples -qqq",
            "cd js_modules/dagit",
            "yarn install",
            "yarn run ts",
            "yarn run jest --collectCoverage --watchAll=false",
            "yarn run check-prettier",
            "yarn run check-lint",
            "yarn run download-schema",
            "yarn run generate-types",
            "git diff --exit-code",
            "mv coverage/lcov.info lcov.dagit.$BUILDKITE_BUILD_ID.info",
            "buildkite-agent artifact upload lcov.dagit.$BUILDKITE_BUILD_ID.info",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]


def helm_steps():
    base_paths = "'helm/dagster/*.yml' 'helm/dagster/*.yaml'"
    base_paths_ignored = "':!:helm/dagster/templates/*.yml' ':!:helm/dagster/templates/*.yaml'"
    return [
        StepBuilder("yamllint helm")
        .run(
            "pip install yamllint",
            "yamllint -c .yamllint.yaml --strict `git ls-files {base_paths} {base_paths_ignored}`".format(
                base_paths=base_paths, base_paths_ignored=base_paths_ignored
            ),
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
        StepBuilder("validate helm schema")
        .run(
            "pip install -e python_modules/automation",
            "dagster-helm schema --command=apply",
            "git diff --exit-code",
            "helm lint helm/dagster -f helm/dagster/values.yaml",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]


###
def integration_suite_extra_cmds_fn(version):
    return [
        'export AIRFLOW_HOME="/airflow"',
        "mkdir -p $${AIRFLOW_HOME}",
        "export DAGSTER_DOCKER_IMAGE_TAG=$${BUILDKITE_BUILD_ID}-" + version,
        'export DAGSTER_DOCKER_REPOSITORY="$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-1.amazonaws.com"',
        "aws ecr get-login --no-include-email --region us-west-1 | sh",
        r"aws s3 cp s3://\${BUILDKITE_SECRETS_BUCKET}/gcp-key-elementl-dev.json "
        + GCP_CREDS_LOCAL_FILE,
        "export GOOGLE_APPLICATION_CREDENTIALS=" + GCP_CREDS_LOCAL_FILE,
        "pushd python_modules/libraries/dagster-celery",
        # Run the rabbitmq db. We are in docker running docker
        # so this will be a sibling container.
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit,
        # Can't use host networking on buildkite and communicate via localhost
        # between these sibling containers, so pass along the ip.
        network_buildkite_container("rabbitmq"),
        connect_sibling_docker_container("rabbitmq", "test-rabbitmq", "DAGSTER_CELERY_BROKER_HOST"),
        "popd",
    ]


def integration_tests():
    tests = []
    # tests += publish_test_images()
    tests += ModuleBuildSpec(
        os.path.join("integration_tests", "python_modules", "dagster-k8s-test-infra"),
        supported_pythons=SupportedPythons,
        upload_coverage=True,
    ).get_tox_build_steps()

    integration_suites_root = os.path.join(SCRIPT_PATH, "..", "integration_tests", "test_suites")
    integration_suites = [
        os.path.join("integration_tests", "test_suites", suite)
        for suite in os.listdir(integration_suites_root)
    ]

    for integration_suite in integration_suites:
        tox_env_suffixes = None
        if integration_suite == os.path.join(
            "integration_tests", "test_suites", "k8s-integration-test-suite"
        ):
            tox_env_suffixes = ["-default", "-markscheduler"]
        elif integration_suite == os.path.join(
            "integration_tests", "test_suites", "celery-k8s-integration-test-suite"
        ):
            tox_env_suffixes = ["-default", "-markusercodedeployment", "-markruncoordinator"]

        tests += ModuleBuildSpec(
            integration_suite,
            env_vars=[
                "AIRFLOW_HOME",
                "AWS_ACCOUNT_ID",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "BUILDKITE_SECRETS_BUCKET",
                "GOOGLE_APPLICATION_CREDENTIALS",
            ],
            supported_pythons=SupportedPythons,
            upload_coverage=True,
            extra_cmds_fn=integration_suite_extra_cmds_fn,
            depends_on_fn=test_image_depends_fn,
            tox_env_suffixes=tox_env_suffixes,
            retries=2,
        ).get_tox_build_steps()
    return tests


###


def python_steps():
    steps = []
    steps += publish_test_images()

    steps += pylint_steps()
    steps += [
        StepBuilder("isort")
        .run("pip install isort>=4.3.21", "make isort", "git diff --exit-code",)
        .on_integration_image(SupportedPython.V3_7)
        .build(),
        StepBuilder("black")
        # See: https://github.com/dagster-io/dagster/issues/1999
        .run("make check_black").on_integration_image(SupportedPython.V3_7).build(),
        StepBuilder("mypy examples")
        .run(
            "pip install mypy",
            # start small by making sure the local code type checks
            "mypy examples/airline_demo/airline_demo "
            "examples/legacy_examples/dagster_examples/bay_bikes "
            "examples/docs_snippets/docs_snippets/intro_tutorial/basics/e04_quality/custom_types_mypy* "
            "--ignore-missing-imports",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
        StepBuilder("Validate Library Docs")
        .run("pip install -e python_modules/automation", "dagster-docs validate-libraries")
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]

    for m in DAGSTER_PACKAGES_WITH_CUSTOM_TESTS:
        steps += m.get_tox_build_steps()

    steps += extra_library_tests()

    # https://github.com/dagster-io/dagster/issues/2785
    steps += pipenv_smoke_tests()
    steps += version_equality_checks()
    steps += next_docs_build_tests()
    steps += examples_tests()
    steps += integration_tests()

    return steps
>>>>>>> Tmp: Enables integration tests in BuildKite.


if __name__ == "__main__":
    main()
