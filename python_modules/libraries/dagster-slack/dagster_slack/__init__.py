from dagster.core.utils import check_dagster_package_version

from .hooks import send_slack_message_on_failure, send_slack_message_on_success
from .resources import slack_resource
from .version import __version__

check_dagster_package_version("dagster-slack", __version__)

__all__ = ["slack_resource"]
