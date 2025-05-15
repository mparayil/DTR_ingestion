# dimensions and/or measurements list by workspace_id (GET)
LIST_MEASUREMENTS_URL = "https://app.datorama.com/v1/workspaces/{workspace_id}/measurements?type=type"
LIST_DIMENSIONS_URL = "https://app.datorama.com/v1/workspaces/{workspace_id}/dimensions?type=dimensionType"

# List data streams by workspace_id (GET)
LIST_DATA_STREAMS_URL = "https://app.datorama.com/v1/workspaces/{workspace_id}/data-streams"

# find report by report_id or workspace_id (GET)
FIND_REPORT_URL = "https://app.datorama.com/v1/reports/{report_id}"
LIST_REPORTS_URL = "https://app.datorama.com/v1/workspaces/{workspace_id}/reports"
DELETE_REPORT_URL = "https://app.datorama.com/v1/reports/{report_id}"

# find report-scheduler by scheduler_id or report_id (GET)
FIND_REPORT_SCHEDULER_URL = "https://app.datorama.com/v1/report-schedulers/{report_scheduler_id}"
LIST_REPORT_SCHEDULERS_URL = "https://app.datorama.com/v1/reports/{report_id}/report-schedulers?includeOnlyActive=false"

# list or find report jobs by report_job_id or report_id (GET)
FIND_REPORT_JOB_STATUS_URL = "https://app.datorama.com/v1/report-jobs/{report_job_id}"
DOWNLOAD_REPORT_URL = "https://app.datorama.com/v1/report-jobs/{report_job_id}/download"
LIST_JOBS_BY_REPORT_ID_URL = "https://app.datorama.com/v1/reports/{report_id}/report-jobs"

# Query API Calls (POST)
QUERY_URL = "https://app.datorama.com/v1/query"
SYSTEM_QUERY_URL = QUERY_URL + "?useSystemNames=true"

# Report API calls
CREATE_REPORT_URL = "https://app.datorama.com/v1/reports" #POST
CREATE_REPORT_SCHEDULER_URL = "https://app.datorama.com/v1/report-schedulers" #POST
PROCESS_REPORT_URL = "https://app.datorama.com/v1/reports/{report_id}/process" #POST
UPDATE_REPORT_URL = "https://app.datorama.com/v1/reports/{report_id}" #PATCH or PUT

DEV_PROJECT = "cp-saa-media-dev"
BQ_CREDENTIALS = "/vault/secrets/gcp-sa-bq.json"
GCS_CREDENTIALS = "/vault/secrets/gcp-sa-storage.json"
DATORAMA_CREDENTIALS = "/vault/secrets/credentials.json"
SCOPES = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/cloud-platform"]
QUERY_API_LIMIT = 400

# Datorama constant variables
NA_WORKSPACE_ID = 83073
APAC_WORKSPACE_ID = 96598
HILLS_WORKSPACE_ID = 117110
EU_WORKSPACE_ID = 108232
LATAM_WORKSPACE_ID = 108232
AED_WORKSPACE_ID = 114958