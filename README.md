## Fraudfinder - A comprehensive lab series on how to build a real-time fraud detection system on Google Cloud.

[Fraudfinder](https://github.com/googlecloudplatform/fraudfinder) is a series of labs on how to build a real-time fraud detection system on Google Cloud. Throughout the Fraudfinder labs, you will learn how to read historical bank transaction data stored in data warehouse, read from a live stream of new transactions, perform exploratory data analysis (EDA), do feature engineering, ingest features into a feature store, train a model using feature store, register your model in a model registry, evaluate your model, deploy your model to an endpoint, do real-time inference on your model with feature store, and monitor your model.


## How to use this repo

This repo is organized across various notebooks as:

* [00_environment_setup.ipynb](00_environment_setup.ipynb)
* [01_exploratory_data_analysis.ipynb](01_exploratory_data_analysis.ipynb)
* [02_feature_engineering_batch.ipynb](02_feature_engineering_batch.ipynb)
* [03_feature_engineering_streaming.ipynb](03_feature_engineering_streaming.ipynb)
* [bqml/](bqml/)
  * [04_model_training_and_prediction.ipynb](bqml/04_model_training_and_prediction.ipynb)
  * [05_model_training_pipeline_formalization.ipynb](bqml/05_model_training_pipeline_formalization.ipynb)
  * [06_model_monitoring.ipynb](bqml/06_model_monitoring.ipynb)


## Running the notebooks

To run the notebooks successfully, follow the steps below.

### Step 1: Enable the Notebooks API

Open Cloud Shell and execute the following code to enable the necessary APIs, and create Pub/Sub subscriptions to read streaming transactions from public Pub/Sub topics.

This step may take a few minutes. 

```shell
gcloud services enable notebooks.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable aiplatform.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com

gcloud pubsub subscriptions create "ff-tx-sub" --topic="ff-tx" --topic-project="cymbal-fraudfinder"
gcloud pubsub subscriptions create "ff-txlabels-sub" --topic="ff-txlabels" --topic-project="cymbal-fraudfinder"

# Give GCS access to service account to deploy Vertex AI Pipelines
PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUM=$(gcloud projects list --filter="$PROJECT_ID" --format="value(PROJECT_NUMBER)")
gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:${PROJECT_NUM}-compute@developer.gserviceaccount.com"\
      --role='roles/storage.admin'
```

You can navigate to the [Pub/Sub console](https://console.cloud.google.com/cloudpubsub/subscription/) to see the subscriptions. 

#### Step 2: Create a User-Managed Notebook instance on Vertex AI Workbench

Click on "+ NEW NOTEBOOK" on [the Vertex AI Workbench page](https://console.cloud.google.com/vertex-ai/workbench/list/instances).

For the instance, select "**Python 3**", select a location, and then click "**CREATE**" to create the notebook instance.

The instance will be ready when you can click on "**OPEN JUPYTERLAB**" on the [User-Managed Notebooks page](https://console.cloud.google.com/vertex-ai/workbench/list/instances). It may take a few minutes for the instance to be ready.

#### Step 3: Open JupyterLab
Click on "**OPEN JUPYTERLAB**", which should launch your Managed Notebook in a new tab.

#### Step 4: Opening a terminal

Open a terminal via the file menu: **File > New > Terminal**.

#### Step 5: Cloning this repo

Run the following code to clone this repo:
```
git clone https://github.com/GoogleCloudPlatform/fraudfinder.git
```

or navigate to the menu on the left in the Jupyter Lab environment -> Git -> Clone a repository.

Once cloned, you should now see the **fraudfinder** folder in your main directory.


#### Step 6: Open the first notebook

Open the first notebook:
- `00_environment_setup.ipynb`

Follow the instructions in the notebook, and continue through the remaining notebooks.
