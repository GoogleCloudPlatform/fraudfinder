from kfp.v2.google.client import AIPlatformClient
import time
import os
import argparse
import json
import logging
from kfp.v2.google.client import AIPlatformClient
from google.cloud import storage
import base64

#DISPLAY_NAME = 'fd-kfp'.format(str(int(time.time())))
PROJECT_ID = os.getenv("PROJECT_ID", '')
BUCKET_NAME = f"{PROJECT_ID}-fraudfinder"
client = storage.Client()
bucket =  client.get_bucket(BUCKET_NAME)
blob = bucket.get_blob('config/notebook_env.py')
config = blob.download_as_string()
exec(config)

PIPELINE_ROOT = f'gs://{BUCKET_NAME}/pipelines'
print(f'''Pipeline Root: {PIPELINE_ROOT}''')

def run_pipeline(pipelines_file_location):
    print(f'''REGION:{REGION}''')
    api_client = AIPlatformClient(project_id=PROJECT_ID,region=REGION,)
    
    response = api_client.create_run_from_job_spec(
        pipelines_file_location, 
        pipeline_root=PIPELINE_ROOT,
        parameter_values={"project_id": PROJECT_ID,
                          "region": REGION,},
        enable_caching=False
    )

    print(response)
    # from google.cloud import aiplatform
    # job = aiplatform.PipelineJob(display_name = 'test1',
    #                              template_path = pipelines_file_location,
    #                              pipeline_root = PIPELINE_ROOT,
    #                              parameter_values = {"project_id": PROJECT_ID,"region": REGION,},
    #                              project = PROJECT_ID,
    #                              location = REGION)
    # job.run(sync=False)



def get_args():
    parser = argparse.ArgumentParser()

    
    parser.add_argument(
        '--pipelines-file-location', 
        type=str,
    )

    return parser.parse_args()

def main():
    args = get_args()

    pipelines_file_location = args.pipelines_file_location
    result = run_pipeline(pipelines_file_location)
    logging.info(result)
        
    
if __name__ == "__main__":
    main()