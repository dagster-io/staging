import os
import smtplib
import ssl
from typing import Callable, List, Optional

from dagster.core.definitions.pipeline_sensor import (
    PipelineFailureSensorContext,
    pipeline_failure_sensor,
)
from dagster.core.errors import DagsterInvalidDefinitionError

SENDER_ADDRESS = "ALERT_EMAIL_ADDRESS"
SENDER_PASSWORD = "ALERT_EMAIL_PASSWORD"


def _default_failure_email_body(context: PipelineFailureSensorContext) -> str:
    return "\n".join(
        [
            f"Pipeline {context.pipeline_run.pipeline_name} failed!",
            f"Run ID: {context.pipeline_run.run_id}",
            f"Error: {context.failure_event.message}",
        ]
    )


EMAIL_MESSAGE = """From: <{email_from}>
To: {email_to}
MIME-Version: 1.0
Content-type: text/html
Subject: {email_subject}

{email_body}
"""


def send_email_via_ssl(
    email_from: str,
    email_password: str,
    email_to: List[str],
    message: str,
    smtp_host: str,
    smtp_port: int,
):
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
        server.login(email_from, email_password)
        server.sendmail(email_from, email_to, message)


def send_email_via_starttls(
    email_from: str,
    email_password: str,
    email_to: List[str],
    message: str,
    smtp_host: str,
    smtp_port: int,
):
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls(context=context)
        server.login(email_from, email_password)
        server.sendmail(email_from, email_to, message)


def make_email_on_pipeline_failure_sensor(
    email_to: List[str],
    email_body_fn: Callable[[PipelineFailureSensorContext], str] = _default_failure_email_body,
    email_subject: Optional[str] = "Dagster Pipeline Failure Alert",
    smtp_host: Optional[str] = "smtp.gmail.com",
    smtp_type: Optional[str] = "SSL",
    smtp_port: Optional[int] = None,
    name: Optional[str] = None,
    dagit_base_url: Optional[str] = None,
):
    """Create a pipeline failure sensor that sends email via the SMTP protocol.

    Args:
        email_to (List[str]): The receipt email addresses to send the message to.
        # email_from (str): The email address to send from.
        email_body_fn (Optional(Callable[[HookContext], str])): Function which takes in the
            ``PipelineFailureSensorContext`` outputs the email body you want to send. Defaults to
            the plain text that contains error message, pipeline name, and run ID.
        email_subject (Optional[str]): The subject of the email. Defaults to "Dagster Pipeline
            Failure Alert".
        smtp_host (Optional[str]): The hostname of the SMTP server. Defaults to "smtp.gmail.com".
        smtp_type (Optional[int]): The protocol; either "SSL" or "STARTTLS". Defaults to SSL.
        smtp_port (Optional[int]): The SMTP port. Defaults to 465 for SSL, 587 for STARTTLS.
        name: (Optional[str]): The name of the sensor. Defaults to "email_on_pipeline_failure".
        dagit_base_url: (Optional[str]): The base url of your Dagit instance. Specify this to allow
            messages to include deeplinks to the specific pipeline run that triggered the hook.

    The configure the email sender, you will need "ALERT_EMAIL_ADDRESS" and "ALERT_EMAIL_PASSWORD"
    env variables set in your environment.

    Examples:

        .. code-block:: python

            email_on_pipeline_failure = make_email_on_pipeline_failure_sensor(
                email_to="xxx@mycoolsite.com",
            )

            @repository
            def my_repo():
                return [my_pipeline + email_on_pipeline_failure]

        .. code-block:: python

            def my_message_fn(context: PipelineFailureSensorContext) -> str:
                return "Pipeline {pipeline_name} failed! Error: {error}".format(
                    pipeline_name=context.pipeline_run.pipeline_name,
                    error=context.failure_event.message,
                )

            email_on_pipeline_failure = make_email_on_pipeline_failure_sensor(
                email_to="xxx@mycoolsite.com",
                message_fn=my_message_fn,
                dagit_base_url="http://mycoolsite.com",
            )


    """

    email_from = os.getenv(SENDER_ADDRESS)
    email_password = os.getenv(SENDER_PASSWORD)

    if email_from is None or email_password is None:
        raise DagsterInvalidDefinitionError(
            f'Env variable "{SENDER_ADDRESS}" or "{SENDER_PASSWORD}" is unset.'
        )

    @pipeline_failure_sensor(name=name)
    def email_on_pipeline_failure(context: PipelineFailureSensorContext):

        message = EMAIL_MESSAGE.format(
            email_to=email_to,
            email_from=email_from,
            email_subject=email_subject,
            email_body=email_body_fn(context),
        )
        if dagit_base_url:
            message += f'<p><a href="{dagit_base_url}/instance/runs/{context.pipeline_run.run_id}">View in Dagit</a></p>'

        if smtp_type == "SSL":
            send_email_via_ssl(
                email_from, email_password, email_to, message, smtp_host, smtp_port=smtp_port or 465
            )
        elif smtp_type == "STARTTLS":
            send_email_via_starttls(
                email_from, email_password, email_to, message, smtp_host, smtp_port=smtp_port or 587
            )
        else:
            raise DagsterInvalidDefinitionError(f'smtp_type "{smtp_type}" is not supported.')

    return email_on_pipeline_failure
