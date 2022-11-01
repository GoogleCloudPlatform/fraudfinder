#General
import os
import sys
import random
import json
from datetime import datetime, timedelta

#Vertex Pipelines
from typing import NamedTuple
import kfp
from kfp.v2 import dsl
from kfp.v2.dsl import Artifact, Dataset, Input, InputPath, Model, Output, OutputPath, Metrics, ClassificationMetrics, Condition, component
from kfp.v2 import compiler
from kfp.v2.google.client import AIPlatformClient as VertexAIClient
import google_cloud_pipeline_components
from google_cloud_pipeline_components import aiplatform as vertex_ai_components

from google.cloud import aiplatform as vertex_ai


from pipeline.lib.batch_serve import features_to_gcs
from pipeline.lib.evaluate_model import evaluate_model
from google.cloud import storage


print("kfp version:", kfp.__version__)
print("Pipeline component version:", google_cloud_pipeline_components.__version__)
print("google-cloud-aiplatform version:",vertex_ai.__version__)


# These variables would be passed from Cloud Build in CI/CD.
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
PROJECT_ID = os.getenv("PROJECT_ID", "")
BUCKET_NAME = f"{PROJECT_ID}-fraudfinder"

client = storage.Client()
bucket =  client.get_bucket(BUCKET_NAME)
blob = bucket.get_blob('config/notebook_env.py')
config = blob.download_as_string()
exec(config)



# TODO to load it from config file
IMAGE_REPOSITORY = f'fraudfinder-{ID}'
IMAGE_NAME='dask-xgb-classificator'
IMAGE_TAG='v1'
IMAGE_URI=f"us-central1-docker.pkg.dev/{PROJECT_ID}/{IMAGE_REPOSITORY}/{IMAGE_NAME}:{IMAGE_TAG}"



#Components
# BASE_IMAGE="gcr.io/google.com/cloudsdktool/cloud-sdk:latest"
BASE_IMAGE='python:3.7'
COMPONENTS_DIR=os.path.join(os.curdir, 'pipelines', 'components')
INGEST_FEATURE_STORE=f"{COMPONENTS_DIR}/ingest_feature_store_{TIMESTAMP}.yaml"
EVALUATE=f"{COMPONENTS_DIR}/evaluate_{TIMESTAMP}.yaml"

#Pipeline
PIPELINE_NAME = f'fraud-finder-xgb-pipeline2-{ID}'
PIPELINE_DIR=os.path.join(os.curdir, 'pipelines')
PIPELINE_ROOT = f"gs://{BUCKET_NAME}/pipelines"
PIPELINE_PACKAGE_PATH = f"{PIPELINE_DIR}/pipeline_{TIMESTAMP}.json"

#Feature Store component
START_DATE_TRAIN = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
END_DATE_TRAIN = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
BQ_DATASET = "tx"
READ_INSTANCES_TABLE = f"ground_truth"
READ_INSTANCES_URI = f"bq://{PROJECT_ID}.{BQ_DATASET}.{READ_INSTANCES_TABLE}"

#Dataset component
DATASET_NAME = f'fraud_finder_dataset_{END_DATE_TRAIN}'

#Training component
JOB_NAME = f'fraudfinder-train-xgb-{TIMESTAMP}'
MODEL_NAME = f'fraudfinder-xgb-{ID}'
TRAIN_MACHINE_TYPE = 'n2-standard-4'
CONTAINER_URI = 'us-docker.pkg.dev/vertex-ai/training/xgboost-cpu.1-1:latest'
MODEL_SERVING_IMAGE_URI = 'us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-1:latest'
PYTHON_MODULE = 'trainer.train_model'
ARGS=["--bucket", f"gs://{BUCKET_NAME}"]

#Evaluation component
METRICS_URI = f"gs://{BUCKET_NAME}/deliverables/metrics.json"
AVG_PR_THRESHOLD = 0.8
AVG_PR_CONDITION = 'avg_pr_condition'

#endpoint
ENDPOINT_NAME = 'fraudfinder_xgb_prediction'


@dsl.pipeline(
pipeline_root=PIPELINE_ROOT,
name=PIPELINE_NAME,)
def pipeline(project_id:str = PROJECT_ID, 
             region:str = REGION, 
             bucket_name:str = f"gs://{BUCKET_NAME}",
             feature_store_id:str = FEATURESTORE_ID, 
             read_instances_uri:str = READ_INSTANCES_URI,
             replica_count:int = 1,
             machine_type:str = "n1-standard-4",
             train_split:float = 0.8,
             test_split:float = 0.1,
             val_split:float = 0.1,
             metrics_uri: str = METRICS_URI, 
             thold: float = AVG_PR_THRESHOLD,
            ):

    #Export data from featurestore
    features_to_gcs_op = features_to_gcs(project_id=project_id, region=region, bucket_name=bucket_name, 
                                             feature_store_id=feature_store_id, read_instances_uri=read_instances_uri)

    #create dataset 
    dataset_create_op = vertex_ai_components.TabularDatasetCreateOp(project=project_id,
                                                       display_name=DATASET_NAME,
                                                       gcs_source=features_to_gcs_op.outputs['snapshot_uri_paths']).after(features_to_gcs_op)

    #custom training job component - script
    train_model_op = vertex_ai_components.CustomContainerTrainingJobRunOp(
        display_name=JOB_NAME,
        model_display_name=MODEL_NAME,
        container_uri=IMAGE_URI,
        staging_bucket=bucket_name,
        dataset=dataset_create_op.outputs['dataset'],
        base_output_dir=bucket_name,
        args = ARGS,
        replica_count= replica_count,
        machine_type= machine_type,
        training_fraction_split=train_split,
        validation_fraction_split=val_split,
        test_fraction_split=test_split,
        model_serving_container_image_uri=MODEL_SERVING_IMAGE_URI,
        project=project_id,
        location=region).after(dataset_create_op)

    #evaluate component
    evaluate_model_op = evaluate_model(model_in=train_model_op.outputs["model"], 
                                       metrics_uri=metrics_uri).after(train_model_op)

    #if threshold
    with Condition(evaluate_model_op.outputs['metrics_thr'] < thold, name=AVG_PR_CONDITION):

        #create endpoint
        create_endpoint_op = vertex_ai_components.EndpointCreateOp(
            display_name=ENDPOINT_NAME,
            project=project_id).after(evaluate_model_op)

        #deploy th model
        custom_model_deploy_op = vertex_ai_components.ModelDeployOp(
            model=train_model_op.outputs["model"],
            endpoint=create_endpoint_op.outputs["endpoint"],
            deployed_model_display_name=MODEL_NAME,
            dedicated_resources_machine_type=machine_type,
        dedicated_resources_min_replica_count=replica_count
        ).after(create_endpoint_op)
