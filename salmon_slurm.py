#!/usr/bin/env python3
import shlex
from argparse import ArgumentParser
from os import fspath
from pathlib import Path
from subprocess import check_call
from typing import List

from data_path_utils import create_slurm_path

pipeline_path = Path(
    "/hive/hubmap/data/CMU_Tools_Testing_Group/pipelines/salmon-rnaseq/pipeline.cwl"
)
singularity_image_dir = Path("/hive/hubmap/data/CMU_Tools_Testing_Group/singularity-images")
scratch_dir_base = Path("/dev/shm/hive-cwl")

template = """
#!/bin/bash

#SBATCH -p batch
#SBATCH --time=1-00:00:00
#SBATCH -c {threads}
#SBATCH -o {output_log}

export CWL_SINGULARITY_CACHE={singularity_image_dir}
{cwltool_command}
rm -rf {tmp_path}
""".strip()

pipeline_command_template = [
    "cwltool",
    "--singularity",
    "--timestamps",
    "--tmpdir-prefix",
    "{tmpdir_prefix}/",
    "--tmp-outdir-prefix",
    "{tmp_outdir_prefix}/",
    "--outdir",
    "{out_dir}",
    "{pipeline_path}",
    "--fastq_dir",
    "{fastq_dir}",
    "--assay",
    "{assay}",
    "--threads",
    "{threads}",
]


def submit_job(dataset_path: Path, assay: str, threads: int, slurm_path: Path, pretend: bool):
    dataset = dataset_path.name
    dest_dir = Path(dataset).resolve()
    dest_dir.mkdir(exist_ok=True, parents=True)

    tmp_path = scratch_dir_base / dataset

    pipeline_command = [
        piece.format(
            tmpdir_prefix=tmp_path / "cwl",
            tmp_outdir_prefix=tmp_path / "cwl-out",
            out_dir=dest_dir,
            pipeline_path=pipeline_path,
            fastq_dir=dataset_path,
            assay=assay,
            threads=threads,
        )
        for piece in pipeline_command_template
    ]
    pipeline_command_str = shlex.join(pipeline_command)

    slurm_script = template.format(
        threads=threads,
        singularity_image_dir=singularity_image_dir,
        output_log=dest_dir / "salmon.log",
        tmp_path=shlex.quote(fspath(tmp_path)),
        cwltool_command=pipeline_command_str,
    )

    script_file = slurm_path / f"{dataset}.sh"
    with open(script_file, "w") as f:
        print(slurm_script, file=f)
    command = ["sbatch", script_file]
    command_str = shlex.join(str(x) for x in command)
    if pretend:
        print("Would run:", command_str)
    else:
        print("Running", command_str)
        check_call(command)


def main(dataset_paths: List[Path], assay: str, threads: int, pretend: bool):
    slurm_path = create_slurm_path("salmon-rnaseq")

    for dataset_path in dataset_paths:
        submit_job(
            dataset_path=dataset_path,
            assay=assay,
            threads=threads,
            slurm_path=slurm_path,
            pretend=pretend,
        )


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("assay")

    input_group = p.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--dataset-list", type=Path)
    input_group.add_argument("--base-dir", type=Path)

    p.add_argument("--threads", type=int, default=16)
    p.add_argument("-n", "--pretend", action="store_true")
    args = p.parse_args()

    if args.dataset_list is not None:
        with open(args.dataset_list) as f:
            dataset_paths = [Path(line.strip()) for line in f]
    elif args.base_dir is not None:
        dataset_paths = list(args.base_dir.iterdir())

    main(
        dataset_paths=dataset_paths,
        assay=args.assay,
        threads=args.threads,
        pretend=args.pretend,
    )
