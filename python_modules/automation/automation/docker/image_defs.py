# pylint: disable=print-call
import contextlib
import os
import shutil
import subprocess

from automation.git import git_repo_root

from dagster import check

from .dagster_docker import DagsterDockerImage


@contextlib.contextmanager
def copy_directories(path_lists, cwd):
    check.invariant(os.path.exists(cwd), 'Images directory does not exist')

    paths_to_copy = []
    for path_list in path_lists:
        src_path = os.path.join(git_repo_root(), *path_list)
        check.invariant(os.path.exists(src_path), 'Path for copying to image build does not exist')

        dest_name = path_list[-1]
        dest_path = os.path.join(cwd, dest_name)

        paths_to_copy.append((src_path, dest_path))

    try:
        for src_path, dest_path in paths_to_copy:
            print('Syncing {} to build dir {}...'.format(src_path, dest_path))
            shutil.copytree(src_path, dest_path)
        yield

    finally:
        for _, dest_path in paths_to_copy:
            shutil.rmtree(os.path.join(dest_path))


@contextlib.contextmanager
def buildkite_integration_cm(cwd):
    '''For the buildkite integration base image, we first copy over scala_modules into the image
    build directory.
    '''
    scala_modules_dir = os.path.join(git_repo_root(), 'scala_modules')
    try:
        cmd = [
            'rsync',
            '-av',
            "--exclude='*target*'",
            "--exclude='*.idea*'",
            "--exclude='*.class'",
            scala_modules_dir,
            '.',
        ]
        print('Syncing scala_modules to build dir...')
        print(cmd)
        subprocess.call(cmd, cwd=cwd)
        yield

    finally:
        shutil.rmtree(os.path.join(cwd, 'scala_modules'))


@contextlib.contextmanager
def k8s_example_cm(cwd):
    try:
        with copy_directories([['examples', 'deploy_k8s', 'example_project']], cwd):
            yield
    finally:
        pass


# Some images have custom build context manager functions, listed here
CUSTOM_BUILD_CONTEXTMANAGERS = {
    'buildkite-integration-base': buildkite_integration_cm,
    'k8s-example': k8s_example_cm,
}


def list_images():
    '''List all images that we manage.

    Returns:
        List[DagsterDockerImage]: A list of all images managed by this tool.
    '''

    images_path = os.path.join(os.path.dirname(__file__), 'images')
    image_folders = [f.name for f in os.scandir(images_path) if f.is_dir()]

    images = []
    for image in image_folders:
        img = DagsterDockerImage(image)
        if image in CUSTOM_BUILD_CONTEXTMANAGERS:
            img = img._replace(build_cm=CUSTOM_BUILD_CONTEXTMANAGERS[image])
        images.append(img)
    return images


def get_image(name):
    '''Retrieve the image information from the list defined above.
    '''
    image = next((img for img in list_images() if img.image == name), None)
    check.invariant(image is not None, 'could not find image {}'.format(name))
    return image
