"""Child process that has a non-zero error code."""

if __name__ == "__main__":
    print("Initial output")  # pylint: disable=print-call
    raise Exception("Subprocess threw an exception")
