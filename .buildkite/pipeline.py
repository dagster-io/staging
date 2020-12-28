# This file is temporary to decouple D5777 from changing the Buildkite web UI
import os


def main():
    os.system(
        "python3 -m pip install --user -e ./buildkite && \
        ~/.local/bin/buildkite-dagster"
    )


if __name__ == "__main__":
    main()
