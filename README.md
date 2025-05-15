# Datorama Report API container

This container pulls Datorama data via the official report API.

Datorama aggregates search traffic, marketing campaign spend, social media marketing campaigns, and other 3rd party marketing platform eCommerce data data into a single funnel. Data structure and model consists of fields harmonized/mapped unique to a specific region/market (aka workspace - LATM, APAC, NA, Hills, etc). Dimensions (categorical/text data) and measurements (numerical data)

## Datorama Data Model Concepts & Overview
#### [Skip to Report API Usage Prerequisites](#prerequisites)
<br>

1. **Data Sources** - API data connectors to 3rd party platforms. 
These can include the following and many more - Facebook Ads Custom, TotalConnect, Youtube-Insights, etc.
    * Can be retrieved from under Account setting in WebUi
         - Account Settings > [Global View](https://support.datorama.com/en/support/solutions/articles/4000112511-global-view) > Datasource Mapping
    * Data dictionary app lookup in `marketplace` of web portal, 
    * Using REST endpoint that lists all datasources in Datorama enterprise account with api key
2. **Data Streams** - One-to-many or many-to-one entity relationships between eCommerce metrics.

3. **Dimensions** - Categorical/Text fields that generally return string or non-numerical data
    - Workspace Dimensions can be retrieved via the `Connect & Mix` tab in WebUI
    - Retrieved from one call to REST api endpoint that lists all dimensions in given workspace_id with use of api key. 
4. **Measurements** - Numerical field types relevant to web metrics such as impressions, clicks, etc.
    - Measurments can be exported as csv/excel file from web portal in the *Connect & Mix* tab.
    - Retrieved from one call to REST api endpoint that lists all measurements in given workspace_id with use of api key.  
**Dimensions** and **Measurements** can be pulled from either the Datorama WebUi after requesting access through the org home page or from calling REST GET api call from platform endpoints.
&nbsp;
> __*All Platform endpoints can be found in the*__ [developer portal](https://developers.datorama.com/docs/manage/dimensions/#overview)

&nbsp;
## Considerations when determining Datorama table schema(s) fields

* When building a datorama report, the whole data model should be considered and taken into account. Identifying which Data Sources & Data Streams are of priority and interest to business users, will build reports that setup the solid foundation for a sensible table schema structure in your respective Datorama workspace. Under the data sources/streams are fields mapped to specific number of  measurments and dimensions. Browse the Datorama success center knowledge portal for more information on the Datorama Data Model.
    * [Best Practices for Building Datorama Reports](https://support.datorama.com/en/support/solutions/articles/4000164494-best-practices-for-datorama-tableau-reports#link1)
* Review the following knowledge portal docs to understand data sources/streams behavior and changes to them cascade down to what's reported on dimensions & measurements.
    * [Datorama Data Model](https://support.datorama.com/en/support/solutions/articles/4000085164-data-model)
    * [Datorama Entity Relationship Mapping](https://support.datorama.com/en/support/solutions/articles/4000134731-datorama-entity-relationship-mapping-bird-s-eye-view)
* Data Dictionary app - provides all Datorama workspace users with access to list of fields available in respective workspace_id. This includes calculated, system, and uncalculated dimensions/measurements. If defined and mapped by business prior, logic for calculated fields are defined as well providing insight into how metrics are calculated
    * [Data Dictionary App access](https://support.datorama.com/en/support/solutions/articles/4000162994-data-dictionary-app#link2)

___________

<br>

## Prerequisites
1. **Datorama Platform Access** - Request SSO Okta access to Datorama platform WebUi portal via the org home page, if not done so already. \

2. **API Authentication** - Reach out to respective business regional/division Datorama  account manager/Client manager to obtain Datorama API token for local testing and experimentation of Platform url endpoints.
    - Usage of the container doesn't require explicit passing of api token since API calls within dag runs reference token stored in Vault path
        > external-data-ingest/datorama/apiKey

3. **Finalized List of Dimensions & Measurements** - Deterimine which Datorama Dimension/Measurements fields (ideally mapped/harmonized by business prior pipeline work) the business use for the table data schema. Global reference to fields can be found and pulled in the following ways
    * An excel/csv spread of all dimensions and measurements can be found under the `Connect & Mix` tab on the WebUi once accesss is accquired.


## Report API Container Arguments

| Container Argument 	| Default Value 	| Required 	| Type 	| Description 	|
|---	|---	|---	|---	|---	|
| --extract_output_bucket_name 	| None 	| True 	| str 	| GCS Bucket name to of where to store datorama raw report extract flat file 	|
| --extract_output_path 	| None 	| True 	| str 	| GCS bucket path or local folder path where to write the raw datorama report extract 	|
| --extract_output_filename 	| None 	| True 	| str 	| fileame to write raw report extract CSV file 	|
| --report_output_bucket_name 	| None 	| True 	| str 	| GCS bucket name to output finalized datorama report flat file 	|
| --report_output_path 	| None 	| True 	| str 	| GCS folder in output bucket to output transformed file to. 	|
| --report_output_filename 	| None 	| True 	| str 	| GCS or local filename to write finalized report to. 	|
| --report_start_date 	| None 	| True 	| str 	| Start date to specify for the beginning of the date range of report coverage in YYYY-mm-dd date format 	|
| --report_end_date 	| None 	| True 	| str 	| End date to specify for the end of the date range of report coverage in YYYY-mm-dd date format 	|
| --workspace_id 	| None 	| True 	| int 	| Numeric identifier identifying region/market in Organization to create the report -  Existing workspaces include global regional markets. Ids can be found in constant.py or datorama portal 	|
| --measurements 	| None 	| True 	| json.loads 	| String List of numerical fields to pass into query and create report from.  Measurements can follow either displayFormat or systemName naming convention but should be only one or the other. NOTE: systemName nomenclature is preferred for field names. 	|
| --dimensions 	| None 	| True 	| json.loads 	| String List of Categorical/Text fields to pass into query and create report from.  Dimensions can follow either displayFormat or systemName naming convention but should be only one or the other. NOTE: systemName nomenclature is preferred for field names. 	|
| --filter_config 	| None 	| False 	| json.loads 	| Json string dictionary specifiying value(s) to filter by Dimension field(s) passed to configuration. Example configuration:   {'*Market [Market Classification]': ['Thailand', 'Vietnam'], '*Obj by ACPL [Campaign Objective Classification]': ['NA', 'A'], 'stringDimensionFiltersOperator': 'OR'} 	|
| --compressed 	| False (if omitted) 	| False 	| boolean flag 	| Toggle flag acting as a boolean to pass into container args. No value needs to be specified after arg flag.  If `--compressed` is passed into dag container args, extract file will be zipped file but if omitted from arguments  extract file have extension declared in runtime. 	|
| --fetch_updates 	| False (if omitted) 	| False 	| boolean flag 	| Toggle flag acting as a boolean to pass into container args. No value needs to be specified after arg flag.  If `--fetch_updates` is passed into dag container args, extract report file will only contain records that were updates/modified and not newly inserted data for that date range. 	|
| --dtype 	| None 	| False 	| json.loads 	| A JSON dictionary of (column: type) pairs to cast column to when reading raw extract file before outputting to report_output_filename 	|

----------------------  
## Example - Report container arguments setup inside DAG

```python
kubernetes_pod_operator.KubernetesPodOperator(
    task_id="load_DTR_report_task",
    name="load_DTR_report_task",
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
        "--compressed",
        "--fetch_updates",
    ]
    annotations=annotations,
    get_logs=True,
    dag=dag,
    resources=compute_resource,
    task_concurrency=1,
    in_cluster=True,
    is_delete_operator_pod=True,
    service_account_name="vault-sidecar",
)
```
&nbsp;


## **Report API Rate Limits**
----------------------

| **API Type**          	| **Time Window** 	| **Max. Allowed Requests** 	|
|-----------------------	|-----------------	|---------------------------	|
| Report API Rate Limit 	| Minute          	| 60                        	|
| Query API Rate Limit  	| Minute          	| 200                       	|
| Daily Quota           	| 24 hours - UTC  	| 20,000                    	|

The daily API call quota includes both Report (Platform) API calls and Query API calls, while the minutely call limit is separate.  

The request limit is done at the user level, not at the account level (e.g. 1 API user in an account allows for 20,000 queries, 2 API users in the account allow for 40,000 queries in that account). The daily quota is enforced no matter which API calls are made. For example, a user who performs 15,000 query API calls + 5,000 platform API calls will reach the daily quota, and any further API calls from that user will be blocked until the end of the time window.

- **Blocked Requests**: Rate-limited calls to our APIs will return a 429 "Too Many Requests" status code. The included response headers X-RateLimit-Reset and X-DailyQuota-Rest indicate the UTC times when the next minute and day intervals open up respectively for new incoming requests. If you intend to exceed the above limits, please contact your account manager.
    * Best Practice is to use a throttle or backoff strategy after being rate limited to avoid triggering rate limits

&nbsp;

## Retrieving Deltas/Updated records Methods 

1. Passing the field `Update Date` alonside the list of Dimensions will show a timestamp of when that row or record was last modified or updated. Using this field, a DAG can be run on a rolling day  window period up to current date to ingest data that might have had updated data streams. A deduped view in bigquery can be used to create table/view with most recent or modified records.

2. Fetching List of Data Streams to in workspace_id to pull via calling the endpoint below. From the list of Data Streams, identify the list of Measurement fields interested in retrieving and their mapped Dimensions. Next, passing the list of Data Streams into the `--filter_config` container argument
> https://app.datorama.com/v1/workspaces/{workspace_id}/data-streams

## Backfills

Backfills can be run using 1 month date range windows to a 3 month period. Due to the large date range period, creation of reports may take some time to full generate since polling job is used to run reports through the platform API. You can manually run a backfill for any start/end dates to JSON config as a variable when triggering via the airflow UI. As such:
```JSON
{"report_start_date": "2020-01-01", "report_end_date": "2020-01-02"}
```
---

# Datorama Further Reading

### **Data Model Entity Relationship - Overview**
![](./img/entity_relationship_mapping.png)

&nbsp;

### **Data Stream Entity Relationships** 
![Data Stream Types](./img/data_stream_types.png)

&nbsp;

1. [Overview of Datorama System Dimension Hierarchies](https://support.datorama.com/en/support/solutions/articles/4000085184-system-dimension-hierarchies)

2. [Datorama Harmonization Mapping Overview](https://support.datorama.com/en/support/solutions/articles/4000084629-mapping).

3. [Data Stream Jobs & Data Types](https://support.datorama.com/en/support/solutions/articles/4000083928-data-stream-jobs-data-stream-type)

<br>

> NOTE: Next version release of report API container aiming to have `--query_by_update_date` flag feature.
