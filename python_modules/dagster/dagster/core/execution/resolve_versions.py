import hashlib

from dagster import check


def resolve_step_input_versions(step, step_versions):
    """Computes and returns the versions for each input defined for a given step.

    If an input is constructed from outputs of other steps, the input version is computed by
    sorting, concatenating, and hashing the versions of each output it is constructed from.

    If an input is constructed from externally loaded input (via run config) or a default value (no
    run config provided), the input version is the version of the provided type loader for that
    input.

    Args:
        step (ExecutionStep): The step for which to compute input versions.
        step_versions (Dict[str, Optional[str]]): Each key is a step key, and the value is the
            version of the corresponding step.

    Returns:
        Dict[str, Optional[str]]: A dictionary that maps the names of the inputs to the provided
            step to their computed versions.
    """

    def _resolve_output_version(step_output_handle):
        """Returns version of step output.

        Step output version is computed by sorting, concatenating, and hashing together the version
        of the step corresponding to the provided step output handle and the name of the output.
        """
        if (
            step_output_handle.step_key not in step_versions
            or not step_versions[step_output_handle.step_key]
        ):
            return None
        else:
            return join_and_hash(
                step_versions[step_output_handle.step_key], step_output_handle.output_name
            )

    input_versions = {}
    for input_name, step_input in step.step_input_dict.items():
        if step_input.is_from_output:
            output_handle_versions = [
                _resolve_output_version(source_handle)
                for source_handle in step_input.source_handles
            ]
            version = join_and_hash(*output_handle_versions)
        else:
            version = step_input.dagster_type.loader.compute_loaded_input_version(
                step_input.config_data
            )

        input_versions[input_name] = version

    return input_versions


def join_and_hash(*args):
    lst = [check.opt_str_param(elem, "elem") for elem in args]
    if None in lst:
        return None
    else:
        unhashed = "".join(sorted(lst))
        return hashlib.sha1(unhashed.encode("utf-8")).hexdigest()


def resolve_config_version(config_value):
    """Resolve a configuration value into a hashed version.

    If a single value is passed in, it is converted to a string, hashed, and returned as the
    version. If a dictionary of config values is passed in, each value is resolved to a version,
    concatenated with its key, joined, and hashed into a single version.

    Args:
        config_value (Union[Any, dict]): Either a single config value or a dictionary of config
            values.
    """
    if not isinstance(config_value, dict):
        return join_and_hash(str(config_value))
    else:
        config_value = check.dict_param(config_value, "config_value")
        return join_and_hash(
            *[key + resolve_config_version(val) for key, val in config_value.items()]
        )
