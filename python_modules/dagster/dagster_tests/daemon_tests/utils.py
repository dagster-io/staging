def run_iteration_to_complete(generator):
    """
    Fully execute daemons' run_iteration generator
    """
    try:
        while True:
            next(generator)
    except StopIteration:
        pass
