# This file is temporary to decouple D5777 from changing the Buildkite web UI. Will be removed
# in a follow-up diff
import subprocess


def main():
    res = subprocess.check_output(
        "python3 -m pip install --user -e .buildkite/dagster-buildkite && \
        ~/.local/bin/dagster-buildkite"
    )

    print(res)  # pylint: disable=print-call


if __name__ == "__main__":
    main()
