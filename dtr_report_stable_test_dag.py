import json
from datetime import datetime, timedelta

from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators import kubernetes_pod as kubernetes_pod_operator
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging


compute_resource = k8s.V1ResourceRequirements(
    requests={
        'memory': '2Gi',
        'cpu': '500m'
    },
    limits={
        'memory': '15Gi',
        'cpu': '4000m'
    }
)

namespace = Variable.get("namespace")
env = Variable.get("landscape-abbv")
PROJECT_ID = Variable.get("gcp-project")


annotations = {
    "vault.hashicorp.com/agent-inject": "true",
    "vault.hashicorp.com/agent-pre-populate-only": "true",
    "vault.hashicorp.com/role": namespace,
    "vault.hashicorp.com/agent-inject-secret-credentials.json": "secret/teams/external-data-ingest/datorama/apiKey",
    "vault.hashicorp.com/agent-inject-template-credentials.json": """{{ with secret "secret/teams/external-data-ingest/datorama/apiKey" }}
    {{ .Data.data | toJSON }}
{{ end }}""",
    "vault.hashicorp.com/agent-inject-secret-gcp-sa-bq.json": "secret/gcp-service-accounts/"
    + PROJECT_ID
    + "/gcp-sa-bq",
    "vault.hashicorp.com/agent-inject-template-gcp-sa-bq.json": """{{ with secret "secret/gcp-service-accounts/"""
    + PROJECT_ID
    + """/gcp-sa-bq" }}
    {{ .Data.data.key | base64Decode }}
{{ end }}""",
    "vault.hashicorp.com/agent-inject-secret-gcp-sa-storage.json": "secret/gcp-service-accounts/"
    + PROJECT_ID
    + "/gcp-sa-storage",
    "vault.hashicorp.com/agent-inject-template-gcp-sa-storage.json": """{{ with secret "secret/gcp-service-accounts/"""
    + PROJECT_ID
    + """/gcp-sa-storage" }}
    {{ .Data.data.key | base64Decode }}
{{ end }}""",
    "vault.hashicorp.com/agent-inject-secret-gcp_credentials.json": "secret/gcp-service-accounts/"
    + PROJECT_ID
    + "/gcp-sa-storage",
    "vault.hashicorp.com/agent-inject-template-gcp_credentials.json": """{{ with secret "secret/gcp-service-accounts/"""
    + PROJECT_ID
    + """/gcp-sa-storage" }}
    {{ .Data.data.key | base64Decode }}
{{ end }}""",
    "vault.hashicorp.com/tls-skip-verify": "true",
}


MEASUREMENTS = [
    "GRPs [Mediatools]",
    "Media Cost",
    "Local Media Cost",
    "Total Conversions",
    "Revenue",
    "Video Completions 25%",
    "Video Completions 50%",
    "Video Completions 75%",
    "Video Fully Played",
    "Interactions",
    "Frequency",
    "Reach",
    "*Visits",
    "Fans (Lifetime)",
    "Website Clicks",
    "Display Measured Impressions [DoubleVerify]",
    "Display Viewable Impressions [DoubleVerify]",
    "Video Measured Impressions [DoubleVerify]",
    "Video Viewable Impressions [DoubleVerify]",
    "Content views with shared items [FB CPAS]",
    "FB Estimated Ad Recall Rate",
    "Added Fans",
    "Dislikes",
    "Video Avg. Duration (Seconds)",
    "*Buy Now Clicks",
    "GA Custom Users [Device]",
    "GA Custom Users [In Market Category]",
    "GA Custom Users [Interest Affinity Category]",
    "Time On Site Total",
    "Daily Visitors",
    "Web Site Transactions",
    "New Visits",
    "Page Views",
    "Visitor Bounce",
    "GA Custom Sessions [Device]",
    "GA Custom Sessions [In Market Category]",
    "GA Custom Sessions [Interest Affinity Category]",
    "Time On Site Avg.",
    "Social Paid Impressions",
    "Control_Positive_Rate [Brand Lift Study]",
    "Exposed_Positive_Response [Brand Lift Study]",
    "Social Organic Impressions",
    "Purchases conversion value for shared items only [USD - FB CPAS]",
    "Purchases conversion value for shared items only [Local Currency - FB CPAS]",
    "Impressions [Adwords Product Type]",
    "Impressions [Adwords Title]",
    "Adwords Shopping Conversion Value",
    "Clicks [Adwords Product Type]",
    "Cost USD [Adwords Product Type]",
    "Cost Local Currency [Adwords Product Type]",
    "Adwords Shopping Conversions",
    "Clicks [Adwords Title]",
    "Cost USD [Adwords Title]",
    "Cost Local Currency [Adwords Title]",
    "Total Events",
    "Adds to cart with shared items [FB CPAS]",
    "Search Impr. share",
    "Fans (Daily)",
    "*Video Views (Paid)",
    "*Video Views (Owned)",
    "*Views_Facebook_Post_Lifetime (Owned)",
    "*Impressions (Paid)",
    "*Impressions (Owned)",
    "*Impressions_Facebook_Post_Lifetime (Owned)",
    "*Clicks (Paid)",
    "*Clicks_Facebook_Post_Lifetime (Owned)",
    "*Likes (Paid)",
    "*Likes (Owned)",
    "*Likes_Facebook_Post_Lifetime (Owned)",
    "*Comments (Paid)",
    "*Comments (Owned)",
    "*Comments_Facebook_Post_Lifetime (Owned)",
    "*Shares (Paid)",
    "*Shares (Owned)",
    "*Share_Facebook_Post_Lifetime (Owned)",
    "*Facebook Post Engagements",
    "*Facebook Engaged Users",
    "Adds to cart conversion value for shared items only [Local Currency - FB CPAS]",
    "Adds to cart conversion value for shared items only [USD - FB CPAS]",
    "Purchases with shared items [FB CPAS]",
    "Absolute TVR [TV Ads]",
    "TVC Length [TV Ads]"
]

DIMENSIONS = [
    "*Obj by ACPL [Campaign Objective Classification]",
    "*Master Brand",
    "*Macro Channel",
    "*Ad Dimension",
    "*Format",
    "*Market [Market Classification]",
    "Day",
    "Campaign Key",
    "Campaign Name",
    "*Campaign",
    "*Campaign Objective [Campaign Objective Classification]",
    "*Sub Brand",
    "*Platform",
    "*Channel",
    "*Category",
    "*Sub Category",
    "Creative Key",
    "Creative Name",
    "*Creative",
    "Media Buy Key",
    "*Target Audience",
    "Campaign Advertiser",
    "Campaign Advertiser ID",
    "Media Buy Name",
    "Creative Concept",
    "*Device Category",
    "Media Buy Targeting Age",
    "Media Buy Targeting Gender",
    "Demo [Mediatools]",
    "Search Keyword",
    "Search Keyword Match Type",
    "Creative Click URL",
    "Creative Image",
    "Creative Format",
    "Format [Brand Lift Study]",
    "Study_Name [Brand Lift Study]",
    "Social Element Key",
    "Social Element Name",
    "Social Post Link",
    "Social Post Link Description",
    "Web Analytics Site Medium",
    "Social Post Type",
    "*Retailer",
    "Final URLs",
    "GA Custom Interest Affinity Category",
    "GA Custom User Age Bracket",
    "GA Custom User Gender",
    "GA Custom Interest In Market Category",
    "Web Analytics Event Category",
    "Web Analytics Page Path",
    "Web Analytics Event Label",
    "Web Analytics Keyword Key",
    "Web Analytics Keyword",
    "Web Analytics Site",
    "Web Analytics Site Campaign",
    "Web Analytics Site URL",
    "Product Title [Adwords Title]",
    "Product Type L3 [Adwords Product Type]",
    "*YouTube Video Link",
    "Web Analytics Site Source",
    "Adwords Campaign Labels",
    "*Search Destination [Search Destination Classification]",
    "Social Post Content",
    "Metric [Brand Lift Study]",
    "Market [Mediatools]",
    "Unit [Mediatools]",
    "Vehicle Creative [Mediatools]",
    "Strategy Name",
    "Copyline [TV Ads]",
    "Data Stream Source Name",
    "Data Stream",
    "Update Date",
    "Data Stream Last Run Date",
    "Data Stream Last Updated By",
    "Data Stream Last Data Date",
    "Data Stream Created Date",
    "Data Stream Last Updated Date",
    "Data Stream Job Data Start",
    "Data Stream Job Data End",
    "Data Stream Job Start Execution Time",
    "Data Stream Job End Execution Time",
    "Data Stream Job Id",
    "Data Stream Last Run Status",
    "Data Stream Status",
    "Data Stream Type"
]


# Container image paths
basepath = "us-east4-docker.pkg.dev/"

DATORAMA_REPORT_API_IMG = basepath + "cp-artifact-registry/media/datorama_report_automation:" + env

FILTER_CONFIG = {"*Market [Market Classification]": ["Thailand", "Vietnam"], "stringDimensionFiltersOperator": "OR"}


SECRET_LOCATION = "/vault/secrets/{}"
GCS_SECRET = SECRET_LOCATION.format("gcp-sa-storage.json")
BQ_SECRET = SECRET_LOCATION.format("gcp-sa-bq.json")
GCP_SECRET = SECRET_LOCATION.format("gcp_credentials.json")

RAW_BUCKET_NAME = "cp-saa-media-{}_datorama".format(env)
RAW_BUCKET_PATH = "raw_daily_report_extracts"
CLEAN_BUCKET_PATH = "clean_daily_report_extracts"

APAC_WORKSPACE_ID = "96598"

PIPELINE_BUCKET_NAME = "cp-saa-media-{0}-pipeline".format(env)
PIPELINE_BUCKET_PATH = "{{dag.dag_id}}/{{run_id}}"

PIPELINE_BUCKET_FILENAME = "apac_thvn_clean_dly_report_test_{}_{}.parquet"
RAW_BUCKET_FILENAME = "apac_thvn_raw_dly_report_test_{}_{}.csv"

# MACRO_CALC_START_DATE = "{{ ( ( execution_date - macros.timedelta(days = 1 )).strftime('%Y-%m-%d') )}}"
# MACRO_CALC_END_DATE = "{{ ( execution_date.strftime('%Y-%m-%d') ) }}"

# MACRO_CONF_START_DATE = "{{ dag_run.conf['report_start_date'] if dag_run.conf.get('report_start_date') else execution_date.strftime('%Y-%m-%d') }}"
# MACRO_CONF_END_DATE = "{{ dag_run.conf['report_end_date'] if dag_run.conf.get('report_end_date') else execution_date.strftime('%Y-%m-%d') }}"

# MACRO_START_DATE = "{{ macros.ds_add(ds, -6) }}"
# MACRO_END_DATE = "{{ macros.ds_add(ds, -6) }}"

# REPORT_START_DATE = f" ' {{{{ dag_run.conf['report_start_date'] | default({MACRO_START_DATE}) }}}} ' "
# REPORT_END_DATE = f"{{{{ dag_run.conf['report_end_date'] | default({MACRO_END_DATE}) }}}}"

# MACRO_SDATE_TEST = "{{ dag_run.conf['report_start_date'] if dag_run.conf.get('report_start_date') else (next_execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
# MACRO_EDATE_TEST = "{{( dag_run.conf['report_end_date'] if dag_run.conf.get('report_end_date') else (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') )}}"
# 
# macro_week_start_date = "{{ ( ( next_execution_date - macros.timedelta(days = (next_execution_date.day_of_week + 7) ) ).strftime('%Y-%m-%d') )}}"
# macro_week_end_date = "{{ ( ( next_execution_date - macros.timedelta(days = (next_execution_date.day_of_week + 1) ) ).strftime('%Y-%m-%d') )}}"
# macro_month_start_date = "{{ ( ( macros.datetime( next_execution_date.year, ( next_execution_date.month), 1)  + macros.dateutil.relativedelta.relativedelta(months=-1)).strftime('%Y-%m-%d') )}}"
# macro_month_end_date = "{{ ( ( macros.datetime( next_execution_date.year, ( next_execution_date.month), 1)   + macros.dateutil.relativedelta.relativedelta(days=-1)).strftime('%Y-%m-%d') )}}"
def create_dag(dag_id, schedule_interval):

    tags = ["datorama_report_testing", "datorama_report_api_v0.2"]

    # airflow initialization params
    args = {
        "owner": "airflow",
        "start_date": datetime(2021, 1, 1),
        "email": ["mark_parayil@colpal.com"],
        "email_on_failure": False,
        "retries": 0,
        "depends_on_past": False,
        "retry_delay": timedelta(minutes=3)
    }

    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        catchup=False,
        tags=tags,
        default_args=args,
        max_active_runs=1,
        concurrency=1
    )

    macro_week_start_date = "((next_execution_date - macros.timedelta(days = (next_execution_date.day_of_week + 6) )).strftime('%Y-%m-%d'))"
    macro_week_end_date = "((next_execution_date - macros.timedelta(days = (next_execution_date.day_of_week))).strftime('%Y-%m-%d'))"

    macro_month_start_date = "((macros.datetime( next_execution_date.year, ( next_execution_date.month), 1)  + macros.dateutil.relativedelta.relativedelta(months=-1)).strftime('%Y-%m-%d'))"
    macro_month_end_date = "((macros.datetime( next_execution_date.year, ( next_execution_date.month), 1)   + macros.dateutil.relativedelta.relativedelta(days=-1)).strftime('%Y-%m-%d'))"

    report_month_start_date = f"{{{{ dag_run.conf.report_month_start_date|default({macro_month_start_date}) }}}}"
    report_month_end_date = f"{{{{ dag_run.conf.report_month_end_date|default({macro_month_end_date}) }}}}"

    report_week_start_date = f"{{{{ dag_run.conf.report_week_start_date|default({macro_week_start_date}) }}}}"
    report_week_end_date = f"{{{{ dag_run.conf.report_week_end_date|default({macro_week_end_date}) }}}}"

    report_start_date = "{{ dag_run.conf.report_start_date|default(macros.ds_add(ds, -15)) }}"
    report_end_date = "{{ dag_run.conf.report_end_date|default(macros.ds_add(ds, -15)) }}"


    def show_dag_run_context(**context):
        dag_run_conf = context.get("dag_run").conf
        conf_start_date = dag_run_conf.get("report_start_date", None)
        conf_end_date = dag_run_conf.get("report_end_date", None)

        if conf_start_date and conf_end_date:
            logging.info("using passed in start_date: " + str(conf_start_date) + "end_date: " + str(conf_end_date))
        else:
            macro_start_date = context["templates_dict"]["report_start_date"]
            macro_end_date = context["templates_dict"]["report_end_date"]
            macro_month_start_date = context["templates_dict"]["report_month_start_date"]
            macro_month_end_date = context["templates_dict"]["report_month_end_date"]
            macro_week_start_date = context["templates_dict"]["report_week_start_date"]
            macro_week_end_date = context["templates_dict"]["report_week_end_date"]


            logging.info(f"dag_run_conf is empty, using templates_dict start_date - {macro_start_date}, end_date - {macro_end_date}")
            logging.info(f"dag_run_conf is empty, using templates_dict start_date - {macro_month_start_date}, end_date - {macro_month_end_date}")
            logging.info(f"dag_run_conf is empty, using templates_dict start_date - {macro_week_start_date}, end_date - {macro_week_end_date}")

    print_dates = PythonOperator(
        task_id="print_dates",
        python_callable=show_dag_run_context,
        provide_context=True,
        templates_dict={
            "report_start_date": report_start_date, "report_end_date": report_end_date,
            "report_month_start_date": report_month_start_date, "report_month_end_date": report_month_end_date,
            "report_week_start_date": report_week_start_date, "report_week_end_date": report_week_end_date
        },
        dag=dag
    )

    dtr_report_pull = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="DTR_report_task",
        name="DTR_report_task",
        image=DATORAMA_REPORT_API_IMG,
        pool="DATORAMA_API",
        startup_timeout_seconds=300,
        namespace=namespace,
        image_pull_policy="Always",
        arguments=[
            "--extract_output_bucket_name",
            RAW_BUCKET_NAME,
            "--extract_output_path",
            PIPELINE_BUCKET_PATH + "/" + RAW_BUCKET_PATH,
            "--extract_output_filename",
            RAW_BUCKET_FILENAME.format(report_start_date, report_end_date),
            "--report_output_bucket_name",
            PIPELINE_BUCKET_NAME,
            "--report_output_path",
            PIPELINE_BUCKET_PATH + "/" + CLEAN_BUCKET_PATH,
            "--report_output_filename",
            PIPELINE_BUCKET_FILENAME.format(report_start_date, report_end_date),
            "--report_start_date",
            report_start_date,
            "--report_end_date",
            report_end_date,
            "--workspace_id",
            APAC_WORKSPACE_ID,
            "--measurements",
            json.dumps(MEASUREMENTS),
            "--dimensions",
            json.dumps(DIMENSIONS),
            "--filter_config",
            json.dumps(FILTER_CONFIG),
            # "--compressed", # compression not neccessary since date range size just one day
            "--fetch_updates",
        ],
        annotations=annotations,
        get_logs=True,
        dag=dag,
        resources=compute_resource,
        in_cluster=True,
        is_delete_operator_pod=True,
        service_account_name="vault-sidecar",
    )

    end = DummyOperator(task_id="end_task", dag=dag)

    # Execute Tasks
    print_dates >> dtr_report_pull >> end
    return dag

schedule_interval_ = "*/7 * * * *"
dag_id_ = "dtr_report_stable_test_dag"
globals()[dag_id_] = create_dag(dag_id=dag_id_, schedule_interval=schedule_interval_)
