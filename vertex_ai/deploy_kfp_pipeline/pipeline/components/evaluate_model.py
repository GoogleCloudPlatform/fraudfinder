
from kfp.v2.dsl import Artifact, Dataset, Input, InputPath, Model, Output, OutputPath, Metrics, ClassificationMetrics, Condition, component
from typing import NamedTuple


@component(
output_component_file='./pipelines/components/evaluate.yaml')
def evaluate_model(
    model_in: Input[Artifact],
    metrics_uri: str,
    meta_metrics: Output[Metrics],
    graph_metrics: Output[ClassificationMetrics],
    model_out: Output[Model]) -> NamedTuple("Outputs",
                                            [("metrics_thr", float),],):

    # Libraries --------------------------------------------------------------------------------------------------------------------------
    import json

    # Variables --------------------------------------------------------------------------------------------------------------------------
    metrics_path = metrics_uri.replace('gs://', '/gcs/')
    labels = ['not fraud', 'fraud']

    # Main -------------------------------------------------------------------------------------------------------------------------------
    with open(metrics_path, mode='r') as json_file:
        metrics = json.load(json_file)

    ## metrics
    fpr = metrics['fpr']
    tpr = metrics['tpr']
    thrs = metrics['thrs']
    c_matrix = metrics['confusion_matrix']
    avg_precision_score = metrics['avg_precision_score']
    f1 = metrics['f1_score']
    lg_loss = metrics['log_loss']
    prec_score = metrics['precision_score']
    rec_score = metrics['recall_score']

    meta_metrics.log_metric('avg_precision_score', avg_precision_score)
    meta_metrics.log_metric('f1_score', f1)
    meta_metrics.log_metric('log_loss', lg_loss)
    meta_metrics.log_metric('precision_score', prec_score)
    meta_metrics.log_metric('recall_score', rec_score)
    graph_metrics.log_roc_curve(fpr, tpr, thrs)
    graph_metrics.log_confusion_matrix(labels, c_matrix)

    ## model metadata
    model_framework = 'xgb.dask'
    model_type = 'DaskXGBClassifier'
    model_user = 'inardini' 
    model_function = 'classification'
    model_out.metadata["framework"] = model_framework
    model_out.metadata["type"] = model_type
    model_out.metadata["model function"] = model_function
    model_out.metadata["modified by"] = model_user

    component_outputs = NamedTuple("Outputs",
                                [("metrics_thr", float),],)

    return component_outputs(float(avg_precision_score))