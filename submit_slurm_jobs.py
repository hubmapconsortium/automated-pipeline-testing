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

pipeline_paths = {
    "rna": Path("salmon-rnaseq/pipeline.cwl"),
    "atac": Path("sc-atac-seq-pipeline/sc_atac_seq_prep_process_analyze.cwl"),
    "codex": Path("codex_pipeline/pipeline.cwl"),
}
pipeline_base_path = Path("/hive/hubmap/data/CMU_Tools_Testing_Group/pipelines/")

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

pipeline_command_templates = {
    "rna": [
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
    "atac": [
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
    "codex": [
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
    modality: str,
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

    pipeline_command_template = pipeline_command_templates[modality]
    pipeline_path = pipeline_base_path / pipeline_paths[modality]
    pipeline_command = [
        piece.format(
            tmpdir_prefix=tmp_path / "cwl",
            tmp_outdir_prefix=tmp_path / "cwl-out",
            out_dir=dest_dir,
            pipeline_path=pipeline_path,
            fastq_dir=dataset_path,
            sequence_directory=dataset_path,
            data_directory=dataset_path,
            assay=assay,
            threads=threads,
            gpus=gpus,
        )
        for piece in pipeline_command_template
    ]
    pipeline_command_str = shlex.join(pipeline_command)
    #pipeline_command_str = " ".join(pipeline_command)


    slurm_script = template.format(
        threads=threads,
        singularity_image_dir=singularity_image_dir,
        output_log=dest_dir / f"{modality}.log",
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


def get_derived_datasets_by_modality(modality: str):
    client = Client('https://cells.api.hubmapconsortium.org/api/')
    derived_uuids = [
        dataset["uuid"]
        for dataset in client.select_datasets(where="modality", has=[modality]).get_list()
    ]
    return derived_uuids


def get_metadata(raw_uuid, derived_uuid, modality):
    json = requests.get(f"https://entity.api.hubmapconsortium.org/entities/{raw_uuid}").json()

    if modality in ["rna", "atac"]:
        metadata = {
            "uuid": raw_uuid,
            "group_name": json["group_name"],
            "assay": json["data_types"][0],
        }
    elif modality in ["codex"]:
        metadata = {
            "uuid": raw_uuid,
            "group_name": json["group_name"],
            "tissue_type": get_tissue_type(derived_uuid),
        }
    return metadata


def get_raw_datasets_and_metadata(modality):
    derived_uuids = get_derived_datasets_by_modality(modality)
    uuid_pairs = [(get_parent_uuid(uuid), uuid) for uuid in derived_uuids]
    metadata_list = [
        get_metadata(uuid_pair[0], uuid_pair[1], modality) for uuid_pair in uuid_pairs
    ]
    return pd.DataFrame(metadata_list)


def get_dataset_subset(modality, num_from_each):
    metadata_df = get_raw_datasets_and_metadata(modality)
    uuids_list = []

    if modality in ["atac", "rna"]:
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

    elif modality in ["codex"]:
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
                    uuids_list.append(random_record["uuid"])
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


def get_path_to_data(modality, dataset_tuple):
    uuid = dataset_tuple[0]
    if modality in ["codex"]:
        return Path(f"/hive/hubmap/data/public/{uuid}/")
    elif modality in ["rna", "atac"]:
        group_name = dataset_tuple[2]
        group_name = group_name.replace(" ", "\ ")
        return Path(f"/hive/hubmap/data/protected/{group_name}/{uuid}/")


# def main(dataset_paths: List[Path], assay: str, threads: int, pretend: bool):
def main(modality, threads=1, gpus=0, pretend=False):

    dataset_tuples = get_dataset_subset(modality, num_from_each=1)

    slurm_path_prefixes = {"rna":"salmon-rnaseq", "atac":"sc-atac-seq", "codex":"codex-pipeline"}
    slurm_path_prefix = slurm_path_prefixes[modality]

    slurm_path = create_slurm_path(slurm_path_prefix)

    for dataset_tuple in dataset_tuples:
        dataset_path = get_path_to_data(modality, dataset_tuple)
        assay = dataset_tuple[1] if len(dataset_tuple) > 1 else None
        submit_job(
            modality=modality,
            dataset_path=dataset_path,
            assay=assay,
            threads=threads,
            gpus=gpus,
            slurm_path=slurm_path,
            pretend=pretend,
        )


if __name__ == "__main__":
    p = ArgumentParser()

    p.add_argument("--modality", type=str)
    p.add_argument("--threads", type=int, default=16)
    p.add_argument("--gpus", type=int, default=0)
    p.add_argument("-n", "--pretend", action="store_true")
    args = p.parse_args()

    main(
        modality=args.modality,
        threads=args.threads,
        gpus=args.gpus,
        pretend=args.pretend,
    )
