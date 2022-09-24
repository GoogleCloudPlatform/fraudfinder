## README.md

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
gcloud pubsub subscriptions create "ff-tx-sub" --topic="ff-tx" --topic-project="cymbal-fraudfinder"
gcloud pubsub subscriptions create "ff-txlabels-sub" --topic="ff-txlabels" --topic-project="cymbal-fraudfinder"
gcloud pubsub subscriptions create "ff-tx-highfraud-sub" --topic="ff-tx-highfraud" --topic-project="cymbal-fraudfinder"
gcloud pubsub subscriptions create "ff-txlabels-highfraud-sub" --topic="ff-txlabels-highfraud" --topic-project="cymbal-fraudfinder"

# Give GCS access to service account to deploy Vertex AI Pipelines
PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUM=$(gcloud projects list --filter="$PROJECT_ID" --format="value(PROJECT_NUMBER)")
gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:${PROJECT_NUM}-compute@developer.gserviceaccount.com"\
      --role='roles/storage.admin'
```

You can navigate to the [Pub/Sub console](https://console.cloud.google.com/cloudpubsub/subscription/) to see the subscriptions. 

### Step 2: Set IAM roles

For the sake of simplicity, let's assume you will use the `Compute Engine default service account`. 

Go to the [IAM Admin](https://console.cloud.google.com/iam-admin), click on `ADD`. 

In the view, search and select `Compute Engine default service account` and then assign the following roles:

    - BigQuery Admin
    - Storage Admin
    - Storage Object Admin
    - Vertex AI Administrator
    - Pub/Sub Admin

#### Step 4: Create a Managed Notebook instance

[Create a Managed Notebook](https://console.cloud.google.com/vertex-ai/workbench/create-managed) on Vertex AI Workbench, with the settings:
- Region us-central1
- Under "**Permission**", select "**Service account**"
- Click on "**Advanced settings**" to reveal more settings:
  - Under "**Security**", ensure that "**Enable terminal**" is selected

Then click "**Create**" to create the notebook instance. The instance will be ready when you can click on "**OPEN JUPYTERLAB**" on the [Managed Notebooks page](https://console.cloud.google.com/vertex-ai/workbench/list/managed) on GCP. It may take a few minutes for the instance to be ready.

#### Step 5: Open JupyterLab
Click on "**OPEN JUPYTERLAB**", which should launch your Managed Notebook in a new tab.

#### Step 6: Opening a terminal

Open a terminal via the file menu: **File > New > Terminal**.

#### Step 7: Cloning this repo

Run the following code to clone this repo:
```
git clone https://github.com/GoogleCloudPlatform/fraudfinder.git
```

or navigate to the menu on the left in the Jupyter Lab environment -> Git -> Clone a repository.

Once cloned, you should now see the **fraudfinder** folder in your main directory.


#### Step 8: Open the first notebook

Open the first notebook:
- `00_environment_setup.ipynb`

Select "Python (local)" as your kernel.

Follow the instructions in the notebook, and continue through the remaining notebooks in the `notebooks` folder.
