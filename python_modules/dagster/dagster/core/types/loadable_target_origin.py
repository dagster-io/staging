from collections import namedtuple

from dagster import check
from dagster.serdes import whitelist_for_serdes


@whitelist_for_serdes
class LoadableTargetOrigin(
    namedtuple(
        "LoadableTargetOrigin",
        "executable_path python_file module_name working_directory attribute use_python_package",
    )
):
    def __new__(
        cls,
        executable_path,
        python_file=None,
        module_name=None,
        working_directory=None,
        attribute=None,
        use_python_package=True,
    ):
        return super(LoadableTargetOrigin, cls).__new__(
            cls,
            executable_path=check.str_param(executable_path, "executable_path"),
            python_file=check.opt_str_param(python_file, "python_file"),
            module_name=check.opt_str_param(module_name, "module_name"),
            working_directory=check.opt_str_param(working_directory, "working_directory"),
            attribute=check.opt_str_param(attribute, "attribute"),
            use_python_package=check.bool_param(use_python_package, "use_python_package"),
        )

    def get_cli_args(self):

        # Need to ensure that everything that consumes this knows about
        # --empty-working-directory and --use-python-package
        args = (
            (
                (
                    ["-f", self.python_file,]
                    + (
                        ["-d", self.working_directory]
                        if self.working_directory
                        else ["--empty-working-directory"]
                    )
                )
                if self.python_file
                else []
            )
            + (["-m", self.module_name] if self.module_name else [])
            + (["-a", self.attribute] if self.attribute else [])
            + (["--use-python-package" if self.use_python_package else "--no-use-python-package"])
        )

        return args
