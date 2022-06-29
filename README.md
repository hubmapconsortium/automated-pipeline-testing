This repository consists primarily of two Python scripts which can be run from the command line.

Before running either script, use conda to activate an environment based on the environment.yml file provided here.
This repository consists primarily of two Python scripts which can be run from the command line.

Before running either script, use conda to activate an environment based on the environment.yml file provided here.

build_singularity_images.py creates singularity images based on the dockerfiles contained within a directory supplied as an input parameter.
It takes two parameters as inputs, one optional, one required.  The required parameter, directory, refers to the directory containing docker files base images on.
It must be run with docker permissions, for example, on l001 or l002 when using HuBMAP HIVE infrastructure.
Example usages:

./build_singularity_images.py --directory /path/to/salmon-rnaseq/
./build_singularity_images.py --directory /path/to/codex-pipeline/ --dest-image-dir /path/to/singularity/directory/

The second usage overwrites the default location for storing singularity images, which is /hive/hubmap/data/CMU_Tools_Testing_Group/singularity-images/

submit_slurm_jobs.py selects a suitable assortment of test datasets for a given pipeline and then submits jobs to slurm for running the pipeline on each dataset
It takes a number of inputs, all of which except for This repository consists primarily of two Python scripts which can be run from the command line.

Before running either script, use conda to activate an environment based on the environment.yml file provided here.

build_singularity_images.py creates singularity images based on the dockerfiles contained within a directory supplied as an input parameter.
It takes two parameters as inputs, one optional, one required.  The required parameter, directory, refers to the directory containing docker files base images on.
It must be run with docker permissions, for example, on l001 or l002 when using HuBMAP HIVE infrastructure.
Example usages:

./build_singularity_images.py --directory /path/to/salmon-rnaseq/
./build_singularity_images.py --directory /path/to/codex-pipeline/ --dest-image-dir /path/to/singularity/directory/

The second usage overwrites the default location for storing singularity images, which is /hive/hubmap/data/CMU_Tools_Testing_Group/singularity-images/

submit_slurm_jobs.py selects a suitable assortment of test datasets for a given pipeline and then submits jobs to slurm for running the pipeline on each dataset
It takes a number of inputs:

pipeline-name : required, a string identifier for the pipeline to be tested.  Must be one of: salmon-rnaseq, sc-atac-seq-pipeline, codex-pipeline, sprm

partition : optional, a string identifier describing which partition on the Bridges cluster to submit the job.  Must be one of EM, RM, RM-shared, GPU, GPU-shared.  If none is supplied, a default will be chosen based on pipeline-name.

pipeline-directory: optional, a path to a directory containing the CWL file to be run, if none is supplied will default to a subdirectory of /hive/hubmap/data/CMU_Tools_Testing_Group/pipelines/

num_from_each: optional, the number of elements to choose from each category (assay, data provider x tissue type), default 1, 

include_uuids: optional, a list of dataset uuids to include in testing, in addition to the ones randomly selected

threads: optional, the number of threads to pass to pipelines which take threads as a parameter, default 16

gpus: optional, the number of gpus to pass to pipelines which take gpu count as a parameter, default 1

Example usages:
./submit_slurm_jobs.py --pipeline-name codex-pipeline
./submit_slurm_jobs.py --pipeline-name salmon-rnaseq --partition RM
./submit_slurm_jobs.py --pipeline-name sc-atac-seq-pipeline --num_from_each 3


The first usage provides only the mandatory pipeline name parameter.  The remainder supply optional parameters to override defaults.