def run_iteration_to_complete(generator):
    try:
        while True:
            next(generator)
    except StopIteration:
        pass
