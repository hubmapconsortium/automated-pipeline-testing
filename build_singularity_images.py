#!/usr/bin/env python3
import shlex
from argparse import ArgumentParser
from pathlib import Path
from subprocess import CalledProcessError, check_call, check_output
from typing import Optional

from multi_docker_build.build_docker_images import (
    get_git_info,
    read_images,
    strip_v_from_version_number,
)

default_image_tag = "latest"
singularity_build_command_template = [
    "singularity",
    "build",
    "{dest_image_path}",
    "docker-daemon://{docker_image}",
]


def build_image(docker_image: str, dest_image_dir: Path):
    # TODO: don't duplicate this functionality from cwltool ↓↓↓
    singularity_image_filename = docker_image.replace("/", "_") + ".sif"
    dest_image_path = dest_image_dir / singularity_image_filename

    command = [
        piece.format(
            dest_image_path=dest_image_path,
            docker_image=docker_image,
        )
        for piece in singularity_build_command_template
    ]
    print("Running", shlex.join(command))
    check_call(command)


def main(directory: Path, dest_image_dir: Path):
    tag = strip_v_from_version_number(get_git_info(directory)["version"]) or default_image_tag
    images: list[tuple[str, Path, dict[str, Optional[str]]]] = read_images(directory)
    for image, dockerfile_path, options in images:
        tagged_image = f"{image}:{tag}"
        build_image(tagged_image, dest_image_dir)


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("--directory", type=Path, default=Path())
    p.add_argument("--dest-image-dir", type=Path, default=Path())
    args = p.parse_args()

    main(args.directory, args.dest_image_dir)
