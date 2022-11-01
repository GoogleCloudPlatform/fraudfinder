import argparse
import os
import sys
import logging
import json
from pipeline import kfp_pipeline
import kfp
from kfp.v2 import compiler


# import os
# def list_files(startpath):
#     for root, dirs, files in os.walk(startpath):
#         level = root.replace(startpath, '').count(os.sep)
#         indent = ' ' * 4 * (level)
#         print('{}{}/'.format(indent, os.path.basename(root)))
#         subindent = ' ' * 4 * (level + 1)
#         for f in files:
#             print('{}{}'.format(subindent, f))

# print("current directory: ", os.getcwd())
# print("list files")
# list_files(os.getcwd())



def compile_pipeline(pipeline_name):
    PIPELINE_PACKAGE_PATH = f"./pipelines/{pipeline_name}.json"


    print("kfp version:", kfp.__version__)
    print("pipeline definition path:", PIPELINE_PACKAGE_PATH)
    print("pipeline name:", pipeline_name)
    #mypipeline = kfp_pipeline.create_pipeline(pipeline_name)
    pipeline_definition = compiler.Compiler().compile(pipeline_func=kfp_pipeline.pipeline, package_path = PIPELINE_PACKAGE_PATH)
    return pipeline_definition

# list_files(os.getcwd())

def get_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--pipeline-name', 
        type=str,
    )
    
    return parser.parse_args()

def main():
    args = get_args()
    result = compile_pipeline(args.pipeline_name)
    logging.info(result)
        
    
if __name__ == "__main__":
    main()