FROM python:3.7
WORKDIR /root

# Installs additional packages
RUN pip install  gcsfs numpy pandas urllib3==1.26.16 scikit-learn google-cloud-aiplatform==1.18.1 kfp==1.8.14 google-cloud-pipeline-components==1.0.25 dask distributed xgboost --upgrade

RUN mkdir /app
RUN mkdir /app/pipeline

COPY pipeline/__init__.py /app/pipeline
COPY pipeline/kfp_pipeline.py /app/pipeline
COPY pipeline/components/batch_serve.py /app/pipeline/components/batch_serve.py
COPY pipeline/components/__init__.py /app/pipeline/components
COPY pipeline/components/evaluate_model.py /app/pipeline/components/evaluate_model.py
COPY pipeline_compile.py /app
COPY pipeline_run.py /app
WORKDIR /app
