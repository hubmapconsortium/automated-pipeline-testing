#!/usr/bin/env python3
import shlex
from argparse import ArgumentParser
from os import fspath
from pathlib import Path
from subprocess import check_call
from typing import List

from hubmap_api_py_client import Client
import requests
import pandas as pd
import random

from data_path_utils import create_slurm_path

supported_pipelines = {"salmon-rnaseq", "sc-atac-seq-pipeline", "codex-pipeline", "sprm"}

gpu_pipelines = {"codex-pipeline"}

partition_params_dict = {
    "EM":{"time":"120:00:00", "cores":48},
    "RM":{"time":"48:00:00", "cores":128},
    "RM-512":{"time":"48:00:00", "cores":128},
    "RM-shared":{"time":"48:00:00", "cores":4},
    "GPU":{"time":"48:00:00", "gpu_type":"v100-16", "gpu_num":1},
    "GPU-shared":{"time":"48:00:00", "gpu_type":"v100-16", "gpu_num":1},
}

default_partitions_dict = {
    "salmon-rnaseq":"RM-shared",
    "sc-atac-seq-pipeline":"RM-shared",
    "codex-pipeline":"GPU-shared",
    "sprm":"EM",
}

pipeline_paths = {
    "salmon-rnaseq": Path("salmon-rnaseq/pipeline.cwl"),
    "sc-atac-seq-pipeline": Path("sc-atac-seq-pipeline/sc_atac_seq_prep_process_analyze.cwl"),
    "codex-pipeline": Path("codex-pipeline/pipeline.cwl"),
    "celldive-pipeline": Path("celldive-pipeline/pipeline.cwl"),
    "sprm": Path("sprm/pipeline.cwl"),
}

modalities_dict = {
    "salmon-rnaseq": "rna",
    "sc-atac-seq-pipeline": "atac",
    "codex-pipeline": "codex",
    "celldive-pipeline": "celldive",
    "sprm": "codex",
}

default_pipeline_base_path = Path("/hive/hubmap/data/CMU_Tools_Testing_Group/pipelines/")

singularity_image_dir = Path("/hive/hubmap/data/CMU_Tools_Testing_Group/singularity-images")
scratch_dir_base = Path("/dev/shm/hive-cwl")

cpu_template = """
#!/bin/bash

#SBATCH -p {partition}
#SBATCH -n {cores}
#SBATCH -t {time}
#SBATCH -c {threads}
#SBATCH -o {output_log}

export CWL_SINGULARITY_CACHE={singularity_image_dir}
{cwltool_command}
rm -rf {tmp_path}
""".strip()

gpu_template = """
#!/bin/bash

#SBATCH -p {partition}
#SBATCH --gpus gpu:{gpu_type}:{gpus}
#SBATCH -t {time}
#SBATCH -o {output_log}

export CWL_SINGULARITY_CACHE={singularity_image_dir}
{cwltool_command}
rm -rf {tmp_path}
""".strip()

pipeline_command_templates = {
    "salmon-rnaseq": [
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
    ],
    "sc-atac-seq-pipeline": [
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
        "--sequence_directory",
        "{sequence_directory}",
        "--assay",
        "{assay}",
        "--threads",
        "{threads}",
    ],
    "celldive-pipeline": [
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
        "--data_directory",
        "{data_directory}",
        "--gpus",
        "{gpus}",
    ],
    "sprm": [
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
        "--image_dir",
        "{image_directory}",
        "--mask_dir",
        "{mask_directory}"
    ],
    "codex-pipeline": [
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
        "--data_directory",
        "{data_directory}",
        "--gpus",
        "{gpus}",
    ],
}


def submit_job(
    pipeline_name: str,
    partition: str,
    pipeline_base_path: Path,
    dataset_path: Path,
    assay: str,
    threads: int,
    gpus: int,
    slurm_path: Path,
    pretend: bool,
):
    dataset = dataset_path.name
    dest_dir = Path(dataset).resolve()
    dest_dir.mkdir(exist_ok=True, parents=True)

    tmp_path = scratch_dir_base / dataset

    pipeline_command_template = pipeline_command_templates[pipeline_name]
    pipeline_path = pipeline_paths[pipeline_name]

    if pipeline_base_path.resolve() != default_pipeline_base_path.resolve():
        pipeline_path = Path(pipeline_path.name)

    pipeline_path = pipeline_base_path / pipeline_path
    pipeline_command = [
        piece.format(
            tmpdir_prefix=tmp_path / "cwl",
            tmp_outdir_prefix=tmp_path / "cwl-out",
            out_dir=dest_dir,
            pipeline_path=pipeline_path,
            fastq_dir=dataset_path,
            sequence_directory=dataset_path,
            data_directory=dataset_path,
            image_directory=dataset_path / "stitched/expressions/",
            mask_directory=dataset_path / "stitched/mask/",
            assay=assay,
            threads=threads,
            gpus=gpus,
        )
        for piece in pipeline_command_template
    ]
    pipeline_command_str = shlex.join(pipeline_command)

    template = gpu_template if pipeline_name in gpu_pipelines else cpu_template
    partition_params = partition_params_dict[partition]

    slurm_script = template.format(
        threads=threads,
        partition=partition,
        cores=partition_params["cores"] if "cores" in partition_params else 0,
        time=partition_params["time"],
        gpu_type=partition_params["gpu_type"] if "gpu_type" in partition_params else "",
        gpus=partition_params["gpus"] if "gpus" in partition_params else 0,
        singularity_image_dir=singularity_image_dir,
        output_log=dest_dir / f"{pipeline_name}.log",
        tmp_path=shlex.quote(fspath(tmp_path)),
        cwltool_command=pipeline_command_str,
    )

    script_file = slurm_path / f"{dataset}.sh"
    with open(script_file, "w") as f:
        print(slurm_script, file=f)
    command = ["sbatch", script_file]
    command_str = shlex.join(str(x) for x in command)
    #command_str = " ".join(str(x) for x in command)

    if pretend:
        print("Would run:", command_str)
    else:
        print("Running", command_str)
        check_call(command)


def get_parent_uuid(derived_uuid: str) -> str:
    json = requests.get(f"https://entity.api.hubmapconsortium.org/entities/{derived_uuid}").json()
    try:
        ancestors = json["direct_ancestors"]
        for ancestor in ancestors:
            if ancestor["entity_type"] == "Dataset":
                return ancestor["uuid"]
    except:
        print(json)


def get_datasets_by_pipeline(pipeline_name: str):
    modality = modalities_dict[pipeline_name]
    client = Client('https://cells.api.hubmapconsortium.org/api/')
    derived_uuids = [
        dataset["uuid"]
        for dataset in client.select_datasets(where="modality", has=[modality]).get_list()
    ]
    return derived_uuids


def get_metadata(raw_uuid, derived_uuid, pipeline_name):
    json = requests.get(f"https://entity.api.hubmapconsortium.org/entities/{raw_uuid}").json()

    uuid = derived_uuid if pipeline_name in ["sprm"] else raw_uuid

    if pipeline_name in ["salmon-rnaseq", "sc-atac-seq-pipeline"]:
        metadata = {
            "uuid": uuid,
            "group_name": json["group_name"],
            "assay": json["data_types"][0],
        }
    elif pipeline_name in ["codex-pipeline", "sprm"]:
        metadata = {
            "uuid": uuid,
            "group_name": json["group_name"],
            "tissue_type": get_tissue_type(derived_uuid),
        }
    return metadata


def get_datasets_and_metadata(pipeline_name):
    derived_uuids = get_datasets_by_pipeline(pipeline_name)
    uuid_pairs = [(get_parent_uuid(uuid), uuid) for uuid in derived_uuids]
    metadata_list = [
        get_metadata(uuid_pair[0], uuid_pair[1], pipeline_name) for uuid_pair in uuid_pairs
    ]
    return pd.DataFrame(metadata_list)


def get_dataset_subset(pipeline_name, include_uuids, num_from_each):
    metadata_df = get_datasets_and_metadata(pipeline_name)
    include_df = metadata_df[metadata_df["uuid"].isin(include_uuids)]
    include_df_list = include_df.to_dict(orient='records')

    metadata_df = metadata_df[~metadata_df["uuid"].isin(include_uuids)]
    uuids_list = []

    if pipeline_name in ["sc-atac-seq-pipeline", "salmon-rnaseq"]:
        for record in include_df_list:
            uuids_list.append(
                (record["uuid"], record["assay"], record["group_name"])
            )
        assays = list(metadata_df["assay"].unique())
        for assay in assays:
            sub_df = metadata_df[metadata_df["assay"] == assay]
            sub_df_list = sub_df.to_dict(orient="records")
            count = 0
            while len(sub_df_list) > 0 and count < num_from_each:
                random_index = random.randint(0, len(sub_df_list) - 1)
                random_record = sub_df_list.pop(random_index)
                uuids_list.append(
                    (random_record["uuid"], random_record["assay"], random_record["group_name"])
                )
                count += 1

    elif pipeline_name in ["codex-pipeline", "sprm"]:
        for record in include_df_list:
            uuids_list.append(
                (record["uuid"],)
            )
        group_names = list(metadata_df["group_name"].unique())
        tissue_types = list(metadata_df["tissue_type"].unique())
        for group_name in group_names:
            for tissue_type in tissue_types:
                sub_df = metadata_df[
                    (metadata_df["group_name"] == group_name)
                    & (metadata_df["tissue_type"] == tissue_type)
                ]
                sub_df_list = sub_df.to_dict(orient="records")
                count = 0
                while len(sub_df_list) > 0 and count < num_from_each:
                    random_index = random.randint(0, len(sub_df_list) - 1)
                    random_record = sub_df_list.pop(random_index)
                    uuids_list.append((random_record["uuid"],))
                    count += 1

    return uuids_list


def get_full_organ_name(abbreviation):
    organs_dict = {
        "HT": "Heart",
        "SI": "Small Intestine",
        "LK": "Kidney",
        "RK": "Kidney",
        "LI": "Large Intestine",
        "LV": "Liver",
        "RL": "Lung",
        "LL": "Lung",
        "PA": "Pancreas",
        "LY": "Lymph Node",
        "SP": "Spleen",
        "TH": "Thymus",
    }
    return organs_dict[abbreviation]


def get_tissue_type(dataset: str, token: str = None) -> str:

    dataset_query_dict = {
        "query": {
            "bool": {
                "must": [],
                "filter": [
                    {"match_all": {}},
                    {"exists": {"field": "files.rel_path"}},
                    {
                        "match_phrase": {
                            "uuid": {"query": dataset},
                        }
                    },
                ],
                "should": [],
                "must_not": [{"match_phrase": {"status": {"query": "Error"}}}],
            }
        }
    }

    dataset_response = requests.post(
        "https://search.api.hubmapconsortium.org/search", json=dataset_query_dict
    )

    hits = dataset_response.json()["hits"]["hits"]

    for hit in hits:
        for ancestor in hit["_source"]["ancestors"]:
            if "organ" in ancestor.keys():
                organ_abbreviation = ancestor["organ"]
                return get_full_organ_name(organ_abbreviation)


def get_path_to_data(pipeline_name, dataset_tuple):
    uuid = dataset_tuple[0]
    if pipeline_name in ["codex-pipeline", "sprm"]:
        return Path(f"/hive/hubmap/data/public/{uuid}/")
    elif pipeline_name in ["salmon-rnaseq", "sc-atac-seq-pipeline"]:
        group_name = dataset_tuple[2]
        group_name = group_name.replace(" ", "\ ")
        return Path(f"/hive/hubmap/data/protected/{group_name}/{uuid}/")


# def main(dataset_paths: List[Path], assay: str, threads: int, pretend: bool):
def main(pipeline_name, partition, pipeline_directory, include_uuids, threads=1, gpus=0, num_from_each=1, pretend=False):

    if pipeline_name not in supported_pipelines:
        raise ValueError(f"pipeline_name must be one of {', '.join(supported_pipelines)}")

    if partition != "default" and partition not in default_partitions_dict:
        raise ValueError(f"partition must be 'default' or one of {', '.join(default_partitions_dict.keys())}")

    if partition == "default":
        partition = default_partitions_dict[pipeline_name]

    if include_uuids is None:
        include_uuids = []

    #@TODO
    #Confirm that included UUIDs are valid and of the appropriate type before proceeding

    dataset_tuples = get_dataset_subset(pipeline_name, include_uuids, num_from_each=num_from_each)

    slurm_path_prefix = pipeline_name

    slurm_path = create_slurm_path(slurm_path_prefix)

    for dataset_tuple in dataset_tuples:
        dataset_path = get_path_to_data(pipeline_name, dataset_tuple)
        assay = dataset_tuple[1] if len(dataset_tuple) > 1 else None
        submit_job(
            pipeline_name=pipeline_name,
            partition=partition,
            pipeline_base_path=pipeline_directory,
            dataset_path=dataset_path,
            assay=assay,
            threads=threads,
            gpus=gpus,
            slurm_path=slurm_path,
            pretend=pretend,
        )


if __name__ == "__main__":
    p = ArgumentParser()

    p.add_argument("--pipeline_name", type=str)
    p.add_argument("--partition", type=str, default="default")
    p.add_argument("--pipeline_directory", type=Path, default=default_pipeline_base_path)
    p.add_argument("--threads", type=int, default=16)
    p.add_argument("--gpus", type=int, default=1)
    p.add_argument("--num_from_each", type=int, default=1)
    p.add_argument("--include_uuids", type=str, nargs='*')
    p.add_argument("-n", "--pretend", action="store_true")
    args = p.parse_args()

    main(
        pipeline_name=args.pipeline_name,
        partition=args.partition,
        pipeline_directory=args.pipeline_directory,
        threads=args.threads,
        gpus=args.gpus,
        pretend=args.pretend,
        num_from_each=args.num_from_each,
        include_uuids=args.include_uuids,
    )
