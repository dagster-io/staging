# pylint: disable=no-value-for-parameter

import datetime
import os
import subprocess
import time
import uuid

import click

DAGSTER_REPO = os.environ["DAGSTER_REPO"]
assert DAGSTER_REPO

ALL_STEPS = ["build_and_push_images", "helm_install", "helm_upgrade", "alert", "port_forward_dagit"]
DEFAULT_STEPS = ["build_and_push_images", "helm_install", "alert", "port_forward_dagit"]


@click.command()
@click.option(
    "--helm-values-path",
    "-p",
    required=True,
    type=click.Path(),
    help="values.yaml for helm install",
)
@click.option(
    "--is-template", is_flag=True, help="Whether your helm values file is a template",
)
@click.option(
    "--images",
    multiple=True,
    default=["k8s-dagit-editable", "k8s-celery-worker-editable", "k8s-example-editable",],
    help="images to build and push",
)
@click.option("--python-version", default="3.7.8", help="py version for images")
@click.option("--helm-deployment", default="test", help="deployment name")
@click.option("--custom-tag", default=f"{os.environ['USER']}-test", help="image tag")
@click.option(
    "--randomize-tag", is_flag=True, help="whether to add a random token to the image tag"
)
@click.option("--k8s-namespace", default="default")
@click.option("--ecr-region", default="us-west-1")
@click.option("--steps", multiple=True, default=DEFAULT_STEPS)
@click.option(
    "--dagster-version",
    required=True,
    help="Version of image to build, must match current dagster version",
)
def run_dev_loop(
    helm_values_path,
    is_template,
    images,
    python_version,
    helm_deployment,
    custom_tag,
    randomize_tag,
    k8s_namespace,
    ecr_region,
    steps,
    dagster_version,
):
    """ Run the common dev loop for personal clusters: build and push images, helm install, and
    port forward Dagit.

    Requires that dagster-image be installed (dagster/python_modules/automation).

    NOTE: You may encounter the Docker error 'The specified item already exists in the keychain'.
    Try disabling 'Securely store Docker logins in macOS keychain' in Docker.
    """

    ns = f"-n {k8s_namespace}"

    if randomize_tag:
        custom_tag = custom_tag + str(uuid.uuid4())[:8]

    def _build_and_push_images():
        print("\n---------- Building and pushing images ----------\n")

        _run_sync(f"aws ecr get-login-password --region {ecr_region} | docker login --username AWS --password-stdin 968703565975.dkr.ecr.us-west-1.amazonaws.com")

        processes = []
        for i, image_name in enumerate(images):
            command = (
                f"time ( time (AWS_REGION={ecr_region} dagster-image build --name {image_name} --python-version {python_version} --dagster-version {dagster_version}) && "
                f"time (AWS_REGION={ecr_region} dagster-image push --name {image_name} --python-version {python_version} --custom-tag {custom_tag}))"
            )
            print(f"Running:\n{command}\n")

            # show output of first command in real time
            stdout_fd = None if i == 0 else subprocess.PIPE

            p = subprocess.Popen(
                command,
                executable="/bin/bash",
                shell=True,
                stdout=stdout_fd,
                stderr=subprocess.STDOUT,
            )
            processes.append((image_name, p))

        for image_name, process in processes:
            print(f"\n---------- {image_name} ----------\n")
            stdout, _ = process.communicate()
            if stdout:
                print(stdout.decode("ascii"))
            assert process.returncode == 0

    def _update_template(helm_values_path):
        import re

        # If the provided helm values file is a template file, we replace all instances of {{ tag }}
        # with the tag we just pushed to ECR
        non_template_path = helm_values_path.replace(".tmpl", "")
        with open(helm_values_path, "r+") as f:
            contents = f.read()
            updated_contents = contents
            memo = contents

            for image_name in images:
                updated_contents = re.sub(
                    "{{{{ {image_name}-tag.*? }}}}".format(image_name=image_name),
                    custom_tag,
                    updated_contents,
                )
                memo = re.sub(
                    "{{{{ {image_name}-tag.*? }}}}".format(image_name=image_name),
                    "{{{{ {image_name}-tag:{custom_tag} }}}}".format(
                        image_name=image_name, custom_tag=custom_tag
                    ),
                    memo,
                )

            regex = "{{ .*?-tag(?:.*?) }}"
            values = re.findall(pattern=regex, string=updated_contents)
            for value in values:
                internal_regex = "{{ .*?-tag:(.*?) }}"
                matches = re.findall(pattern=internal_regex, string=value)
                if not len(matches):
                    raise Exception("Image has not been built before.")

                match = matches[0]
                updated_contents = updated_contents.replace(value, match)

            with open(non_template_path, "w") as wf:
                wf.write(updated_contents)

            with open(helm_values_path, "w") as wf:
                wf.write(memo)

        return non_template_path

    def _helm_install(helm_values_path):

        print("\n---------- Helm ----------\n")

        # Helm delete will throw an error if the deployment does not exist
        try:
            _run_sync(
                f"helm delete {helm_deployment} {ns}; kubectl delete jobs --all {ns}", check=False,
            )
        except Exception as e:  # pylint: disable=broad-except
            print("Skipping `helm delete` due to {}".format(str(e)))  # pylint: disable=print-call

        if is_template:
            helm_values_path = _update_template(helm_values_path)

        _run_sync(
            f"helm install {helm_deployment} {DAGSTER_REPO}/helm/dagster -f {helm_values_path} {ns}",
        )

    def _helm_upgrade(helm_values_path):
        print("\n---------- Helm ----------\n")  # pylint: disable=print-call

        if is_template:
            helm_values_path = _update_template(helm_values_path)

        _run_sync(
            f"helm upgrade {helm_deployment} {DAGSTER_REPO}/helm/dagster -f {helm_values_path} {ns}",
        )

    def _alert():
        _run_sync("tput bel", check=False)

    def _port_forward_dagit():
        print("\n---------- Port forwarding ----------\n")  # pylint: disable=print-call

        command = (
            f'export DAGIT_POD_NAME=$(kubectl get pods {ns} -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance={helm_deployment},component=dagit" -o jsonpath="{{.items[0].metadata.name}}")\n'
            'echo "Waiting for pod $DAGIT_POD_NAME to be ready"\n'
            f'while [[ $(kubectl get pods {ns} $DAGIT_POD_NAME -o "jsonpath={{..status.conditions[?(@.type==\'Ready\')].status}}") != "True" ]]; do echo "waiting for pod" && sleep 1; done\n'
            'echo "Visit http://127.0.0.1:8080 to open Dagit"\n'
            f"kubectl {ns} port-forward $DAGIT_POD_NAME 8080:80"
        )

        _run_sync(command)

    for step in steps:
        assert step in ALL_STEPS, "step {} is not in {}".format(step, ALL_STEPS)

    start_time = time.time()

    if "build_and_push_images" in steps:
        _build_and_push_images()

    if "helm_install" in steps:
        _helm_install(helm_values_path)

    if "helm_upgrade" in steps:
        _helm_upgrade(helm_values_path)

    end_time = time.time()
    elapsed_seconds = int(end_time - start_time)
    print(
        "\n\n********** Done **********\n"
        f"********** Time: {datetime.timedelta(seconds=elapsed_seconds)} **********\n"
    )  # pylint: disable=print-call

    if "alert" in steps:
        _alert()

    if "port_forward_dagit" in steps:
        _port_forward_dagit()


def _run_sync(command, check=True, **kwargs):
    command = command.strip()
    print(f"Running:\n{command}\n")
    return subprocess.run(command, shell=True, check=check, **kwargs)


if __name__ == "__main__":
    run_dev_loop()
