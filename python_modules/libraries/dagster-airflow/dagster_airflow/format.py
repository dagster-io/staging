from dagster import check
from dagster.utils.indenting_printer import IndentingStringIoPrinter


def format_dict_for_graphql(dict_):
    """This recursive descent thing formats a dict for GraphQL."""

    def _format_subdict(dict_, current_indent=0):
        check.dict_param(dict_, "dict_", key_type=str)

        printer = IndentingStringIoPrinter(indent_level=2, current_indent=current_indent)
        printer.line("{")

        n_elements = len(dict_)
        for i, key in enumerate(sorted(dict_, key=lambda x: x[0])):
            value = dict_[key]
            with printer.with_indent():
                formatted_value = (
                    _format_item(value, current_indent=printer.current_indent)
                    .lstrip(" ")
                    .rstrip("\n")
                )
                printer.line(f"{key}: {formatted_value}{',' if i != n_elements - 1 else ''}")
        printer.line("}")

        return printer.read()

    def _format_sublist(dict_, current_indent=0):
        printer = IndentingStringIoPrinter(indent_level=2, current_indent=current_indent)
        printer.line("[")

        n_elements = len(dict_)
        for i, value in enumerate(dict_):
            with printer.with_indent():
                formatted_value = (
                    _format_item(value, current_indent=printer.current_indent)
                    .lstrip(" ")
                    .rstrip("\n")
                )
                printer.line(f"{formatted_value}{',' if i != n_elements - 1 else ''}")
        printer.line("]")

        return printer.read()

    def _format_item(dict_, current_indent=0):
        printer = IndentingStringIoPrinter(indent_level=2, current_indent=current_indent)

        if isinstance(dict_, dict):
            return _format_subdict(dict_, printer.current_indent)
        elif isinstance(dict_, list):
            return _format_sublist(dict_, printer.current_indent)
        elif isinstance(dict_, bool):
            return repr(dict_).lower()
        else:
            return repr(dict_).replace("'", '"')

    check.dict_param(dict_, "dict_", key_type=str)
    if not isinstance(dict_, dict):
        check.failed(f"Expected a dict to format, got: {repr(dict_)}")

    return _format_subdict(dict_)
