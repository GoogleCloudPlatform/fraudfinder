"""
Component for interacting with the feature store
"""

from kfp.v2.dsl import Dataset, Input, Output, Artifact, component
from typing import NamedTuple

# COMPONENTS_DIR=os.path.join(os.curdir, 'pipelines', 'components')
# COMPONENT_URI=f"{COMPONENTS_DIR}/features_to_gcs.yaml"

@component(output_component_file='./pipelines/components/features_to_gcs.yaml', 
       base_image='python:3.7', 
       packages_to_install=["git+https://github.com/googleapis/python-aiplatform.git@main"])

def features_to_gcs(project_id:str, region:str, bucket_name:str, feature_store_id: str, read_instances_uri:str) -> NamedTuple("Outputs", [("snapshot_uri_paths", str),],):

    # Libraries --------------------------------------------------------------------------------------------------------------------------
    from datetime import datetime
    import glob
    import urllib
    import json

    #Feature Store
    from google.cloud.aiplatform import Featurestore, EntityType, Feature

    # Variables --------------------------------------------------------------------------------------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    api_endpoint = region + "-aiplatform.googleapis.com"
    bucket = urllib.parse.urlsplit(bucket_name).netloc
    export_uri = f'{bucket_name}/data/snapshots/{timestamp}' #format as new gsfuse requires
    export_uri_path = f'/gcs/{bucket}/data/snapshots/{timestamp}' 
    event_entity = 'event'
    customer_entity = 'customer'
    terminal_entity = 'terminal'
    serving_feature_ids = {customer_entity: ['*'], terminal_entity: ['*']}

    # Main -------------------------------------------------------------------------------------------------------------------------------

    ## Set a client for Feature store managment

    ### Create admin_client for create, read, update and delete (CRUD)
    feature_store_resource_path = f"projects/{project_id}/locations/{region}/featurestores/{feature_store_id}"
    print("Feature Store: \t", feature_store_resource_path)

    ## Run batch job request
    try:
        ff_feature_store = Featurestore(feature_store_resource_path)
        ff_feature_store.batch_serve_to_gcs(
            gcs_destination_output_uri_prefix = export_uri,
            gcs_destination_type = 'csv',
            serving_feature_ids = serving_feature_ids,
            read_instances_uri = read_instances_uri,
            pass_through_fields = ['tx_fraud','tx_amount']
        )
    except Exception as error:
        print(error)

    #Store metadata
    snapshot_pattern = f'{export_uri_path}/*.csv'
    snapshot_files = glob.glob(snapshot_pattern)
    snapshot_files_fmt = [p.replace('/gcs/', 'gs://') for p in snapshot_files]
    snapshot_files_string = json.dumps(snapshot_files_fmt)

    component_outputs = NamedTuple("Outputs",
                                [("snapshot_uri_paths", str),],)

    return component_outputs(snapshot_files_string)

