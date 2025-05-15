import argparse
import json
import logging
import os, sys
import platform
import traceback
from io import BytesIO
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import zipfile

import pandas as pd
import requests
from containers.datorama_report_automation.scripts import constant
from dataEng_container_tools.exceptions import handle_exception
from dataEng_container_tools.gcs import gcs_file_io, get_secrets
from google.cloud import storage
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException, Timeout, ConnectionError
from urllib3.util.retry import Retry
from urllib.request import urlretrieve

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)

config_path = os.getcwd() + "/containers/datorama_report_automation/config"


def running_local():
    return platform.system() == "Darwin"


def gcs_auth_local():
    from pydata_google_auth import cache as py_cache
    from pydata_google_auth import get_user_credentials
    credentials = get_user_credentials(scopes=constant.SCOPES, credentials_cache=py_cache.READ_WRITE)
    storage_client = storage.Client(project=constant.DEV_PROJECT, credentials=credentials)
    return storage_client


def gcs_auth_client(credentials_path: Optional[str] = None):
    if not running_local():
        gcs_io = gcs_file_io(gcs_secret_location=constant.GCS_CREDENTIALS)
    elif running_local() and credentials_path is None:
        gcs_io = gcs_file_io(gcs_secret_location=constant.GCS_CREDENTIALS, local=True)
        gcs_io.gcs_client = gcs_auth_local(); gcs_io.local = False
    elif running_local() and credentials_path is not None:
        gcs_io = gcs_file_io(gcs_secret_location=credentials_path, local=False)
    else:
        gcs_io = gcs_file_io(gcs_secret_location=constant.GCS_CREDENTIALS, local=True)

    gcp_project = gcs_io.gcs_client.project
    msg = f"running from user credentials local environmnent with project {gcp_project}" if running_local() \
        else f"running from on prem environment with project {gcp_project} and local flag as {gcs_io.local}"
    logging.info(msg)
    return gcs_io


def get_api_key():
    logging.info("Trying to read API key from within the container credential json")
    if not running_local():
        creds = get_secrets(constant.DATORAMA_CREDENTIALS)
        creds = json.loads(creds["key"])
        try:
            logging.info("Looking for api key")
            api_key = creds["key"]
            return api_key
        except TypeError as te:
            logging.error(f"secrets not matching object type {te}", exc_info=True)
        except Exception as e:
            logging.exception(f"Could not read STAT api_key from annotations, failed with exception {e}", stack_info=True)
    else:
        logging.info("pulling api key from local machine environment")
        return os.getenv("DATORAMA_API")


def requests_retry_session(retries=1, backoff_factor=0.3, session=None):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries,
                  backoff_factor=backoff_factor, allowed_methods=False)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def make_path(*paths: str):
    # region
    """
    creates paths on local disk if they don't exist otherwise ignores if path exists

    Parameters
    ----------
    paths : str
        *args number of paths can either be a single string path or sequence of string paths
    """
    # endregion
    for path_name in paths:
        try:
            os.makedirs(path_name, exist_ok=False)
        except OSError as oe:
            logging.error(f"unsuccessful creation of directory path since it already exists - {path_name} with error - {oe}", stack_info=True)
        except Exception as e:
            logging.exception("path couldn't be created, error traceback - %e", e)
        else:
            logging.info("Successfully created the directory path = {}".format(path_name))


def read_data_from_file(input_filename: str, input_path: Optional[str] = None, input_bucket_name: Optional[str] = None):
    # region
    """
    Reads data into memory from file from local or GCS bucket path. 
    This func returns dictionary object if json file or pandas dataframe if other tabular file type into memory

    Parameters
    ----------
    input_filename : str
        filename to read from locally or GCS bucket path. 
        File should ideally be csv or json otherwise file will attempt to be read as dataframe depending on file extension
    input_path : Optional[str]
        directory path or GCS bucket path to read data from file into memory. 
        If not provided, func will read from path or filename specified in 'input_filename' arg, by default None
    input_bucket_name : Optional[str]
        GCS bucket name to read data from file if specified. 
        Otherwise func will attempt to read from local , by default None

    Returns
    -------
    Union[Dict, pd.DataFrame]
        Returns data from filename passed in arguments either as python dictionary if data coming from JSON file. 
        Otherwise, will return data as dataframe if filename not ending in ".json"
    """
    # endregion
    if input_path:
        read_path = input_path + "/" + input_filename
    else:
        read_path = input_filename

    gcs_io = gcs_auth_client()
    uri = f"gs://{input_bucket_name}/{input_path}/{input_filename}"
    if input_bucket_name:
        df = gcs_io.download_file_to_object(gcs_uri=uri)
        logging.info("loading data from GCS directly to memory")
    elif not input_bucket_name:
        gcs_io.local = True
        df = gcs_io.download_file_to_object(gcs_uri=read_path)
        logging.info("data read from local disk file path and read into memory - {}".format(read_path))
    else:
        read_bucket = gcs_io.gcs_client.bucket(input_bucket_name)
        blob = read_bucket.get_blob(blob_name=read_path)
        if blob.name.endswith("json"):
            df = json.loads(blob.download_as_bytes())
        elif blob.name.endswith("xlsx"):
            df = pd.read_excel(blob.download_as_bytes())
        elif blob.name.endswith("parquet"):
            df = pd.read_parquet(BytesIO(blob.download_as_bytes()))
        else:
            df = pd.read_csv(BytesIO(blob.download_as_bytes()))

    return df


#TODO partition_report_by: str = ""
@handle_exception
def report_config_generator(output_filename: str, report_start_date: str, report_end_date: str, workspace_id: int, measurements: List, \
    dimensions: List, filter_config: Optional[Dict] = None, compressed: bool = False, fetch_updates: bool = False):

    # reading in report config template from json file on container disk
    headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": get_api_key()}
    if running_local():
        with open(config_path + "/report_config_template.json", "r") as rfile:
            report_config = json.load(rfile)
    else:
        report_config = read_data_from_file(input_filename="report_config_template.json", input_path=config_path)

    # Adding report file export defaut and/or container arguments configuration
    report_config["exportFormat"] = "CSV"
    report_config["shared"] = False
    report_config["workspaceId"] = workspace_id
    report_name = output_filename.split(".")[0]
    # final_report_name = f"{report_name}_{{DATA_START_DATE}}_{{DATA_END_DATE}}"
    report_config["name"] = report_name

    report_config["active"] = True
    report_config["exportFileName"] = report_name

    report_config["includeHeaders"] = True
    report_config["compressed"] = compressed

    # Adding date parameters
    #TODO lag_date = (datetime.strptime(report_start_date, "%Y-%m-%d") - timedelta(days=14)).strftime("%Y-%m-%d") for query_update_date
    date_config = report_config["config"]["filter"]["date"]
    config = report_config["config"]
    date_config["startDate"] = report_start_date
    date_config["endDate"] = report_end_date

    config["queryByUpdateDate"] = fetch_updates
    # adding split_report_by and update_data settings
    config["validMeasure"] = False
    config["splitFilesBy"] = ""

    # mapping dimensions and measurements fields to report config with displayName and systemName formats
    workspace_dimensions = requests_retry_session().get(url=constant.LIST_DIMENSIONS_URL.format(workspace_id=workspace_id), headers=headers).json()
    workspace_measurements = requests_retry_session().get(url=constant.LIST_MEASUREMENTS_URL.format(workspace_id=workspace_id), headers=headers).json()

    system_dimensions = []
    for dd in dimensions:
        for sname, dim in workspace_dimensions.items():
            if dd == dim["displayName"]:
                system_dimensions.append(sname)

    system_measurements = []
    for measure in measurements:
        for sname, m in workspace_measurements.items():
            if measure == m["displayName"]:
                system_measurements.append(dict(name=m["systemName"]))


    config["rowDims"] = system_dimensions
    config["sortInfo"]["metric"] = system_dimensions[0]
    config["sortInfo"]["sortOrder"] = "DESC"

    config["metrics"] = system_measurements

    for rdim in config["rowDims"]:
        for sname, dim in workspace_dimensions.items():
            if rdim == sname:
                config["customDisplayNames"].update(
                    {rdim: dict(defaultName=dim["displayName"], displayName=None, displayFormat=None)}
                )

    for met in config["metrics"]:
        for sname, m in workspace_measurements.items():
            if met["name"] == sname:
                config["customDisplayNames"].update(
                    {met["name"]: dict(defaultName=m["displayName"], displayName=None, displayFormat=None)}
                )

    if filter_config:
        logging.info(f"filter_config passed as - {filter_config}")
        config["filter"]["stringDimensionFiltersOperator"] = filter_config["stringDimensionFiltersOperator"]

        filter_dims = {}
        field_mappings = config["customDisplayNames"]
        for k, val in filter_config.items():
            for name, display in field_mappings.items():
                if (k == display["defaultName"]) or (k == name):
                    logging.info(f"mapping as such {display['defaultName']} -> {k}")
                    filter_dims[name] = {"vals": val, "qVals": None, "emptyQVals": None}

        config["filter"]["filterDims"] = filter_dims
    else:
        logging.info("no filter config argument passed, report selecting all columns")
    return report_config


def download_url_file(download_url: str, headers: Dict):
    try:
        response = requests_retry_session().get(url=download_url, stream=True, headers=headers)
        url_file_path = response.headers["Content-Disposition"].split("\"")[1]
        logging.info(f"response status - {response.raise_for_status()}, url filename path = {url_file_path}")
        handle = open(url_file_path, "wb")
        for chunk in response.iter_content(chunk_size=512):
            if chunk:
                handle.write(chunk)
        handle.close()
    except (HTTPError, requests.exceptions.ConnectTimeout, ConnectionError) as hr:
        logging.exception(f"{hr}")
        try:
            logging.info("trying url retrieve method now instead...")
            result = urlretrieve(url=download_url)
        except RequestException as re:
            logging.exception(f"{re}")
        else:
            logging.info("retrieve method worked and returning file path")
            return result[0]
    else:
        return url_file_path


def upload_file_from_disk(gcs_client, bucket_name, bucket_path, bucket_filename, source_filename, content_type="text/csv", chunk_size_mb=25, timeout=300):
    upload_bucket = gcs_client.get_bucket(bucket_name)
    chunk_size = 1024 * 1024 * chunk_size_mb  # 25 MB
    blob = upload_bucket.blob(bucket_path + "/" + bucket_filename, chunk_size=chunk_size)
    blob.upload_from_filename(source_filename, content_type=content_type, timeout=timeout)


def get_skip_row_num(filename_path: str):
    if filename_path.endswith("zip"):
        zip_obj = zipfile.ZipFile(filename_path, "r")
        report_lines = zip_obj.read(zip_obj.namelist()[0]).splitlines()
    else:
        report_lines = open(filename_path, "r").readlines()

    for i, line in enumerate(report_lines):
        if not line or line in (b"\n", "\n"):
            skip_num = i
            break
        else:
            logging.info(f"row {i} - {line}")

    return skip_num


@handle_exception
def cleanup_old_reports(workspace_id: int, headers: Dict):
    removed_reports = []
    reports_to_delete = []
    list_reports_url = constant.LIST_REPORTS_URL.format(workspace_id=workspace_id)

    reports_res = requests_retry_session().get(url=list_reports_url, headers=headers)
    if reports_res.status_code == 200 and reports_res.ok:
        workspace_reports = reports_res.json()
    else:
        logging.error(f"unable to return reports in datorama workspace {reports_res.raise_for_status()}")

    for report in workspace_reports:
        report_created_time = report["createTime"]
        delete_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        report_create_date = datetime.fromtimestamp(report_created_time/1000.0).strftime("%Y-%m-%d")
        if report["name"] == report["exportFileName"] and report_create_date < delete_date:
            logging.info(f"report name: {report['name']}, report export filename: {report['exportFileName']} and report create date: {report_create_date}")
            reports_to_delete.append(report["id"])
    logging.info(f"{len(reports_to_delete)} reports to remove/delete in workspace to avoid exceeding report limit")

    if len(reports_to_delete) > 0:
        try:
            for report_id in reports_to_delete:
                logging.info(f"attempting to remove report_id - {report_id}")
                delete_report_url = constant.DELETE_REPORT_URL.format(report_id=report_id)
                report_delete_resp = requests_retry_session().delete(url=delete_report_url, headers=headers)
                if report_delete_resp.status_code == 204:
                    logging.info(f"report_id {report_id} successfully deleted.. continuing through report list to remove")
                    removed_reports.append(report_id)
                    time.sleep(3.5)
                else:
                    logging.exception(f"report_id {report_id} failed to be removed from workspace")
        except (HTTPError, RequestException, requests.exceptions.ConnectTimeout, ConnectionError) as hrr:
            logging.exception(f"{hrr}")
        else:
            num_removed_reports = len(removed_reports)
            logging.info(f"number of removed reports at {num_removed_reports}, {removed_reports}")
    else:
        logging.info("no reports to delete in workspace, report limit not reached yet...")


@handle_exception
def main_datorama_report(args):
    api_key = get_api_key()

    headers = {"Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "{0}".format(api_key)}

    extract_fn = args.extract_output_filename
    report_fn = args.report_output_filename

    extract_path = args.extract_output_path
    report_path = args.report_output_path

    extract_bucket = args.extract_output_bucket_name
    report_bucket = args.report_output_bucket_name

    # dates = {"report_start_date": None, "report_end_date": None}
    report_start = args.report_start_date
    report_end = args.report_end_date
    logging.info(f"date range start {report_start} to end {report_end}")

    workspace_id = int(args.workspace_id)
    compressed = bool(args.compressed)
    fetch_updates = bool(args.fetch_updates)
    filter_config = args.filter_config

    dims = args.dimensions
    measures = args.measurements

    #BUG query_update_date arg not pulling deltas in report_api, fix in next container release
    report_config = report_config_generator(output_filename=extract_fn, report_start_date=report_start, report_end_date=report_end, workspace_id=workspace_id, \
        measurements=measures, dimensions=dims, filter_config=filter_config, compressed=compressed, fetch_updates=fetch_updates)

    # process report_config and generate report_id and report in API backend
    try:
        create_report_response = requests_retry_session(retries=0).post(url=constant.CREATE_REPORT_URL, json=report_config, headers=headers)
        if create_report_response.status_code == 201 and create_report_response.ok:
            cr_body = create_report_response.json()
        else:
            logging.error(f"create report config container template failed to generate in workspace {create_report_response.raise_for_status()}")
            raise HTTPError
    except (HTTPError, TimeoutError) as ht:
        logging.exception(f"http exception - {ht}", stack_info=True)
    else:
        report_id = cr_body["id"]
        report_name = cr_body["name"]
        logging.info(f"now getting report_id and global workspace report_name - {report_id} & {report_name}")
    process_report_url = constant.PROCESS_REPORT_URL.format(report_id=report_id)

    # Run report and process it to be generated as export
    try:
        process_response = requests_retry_session(retries=0).post(url=process_report_url, headers=headers)
        logging.info(f"{process_response.raise_for_status()}")
        if process_response.status_code == 200:
            pr_body = process_response.json()
        else:
            logging.info(f"processing report response failed - {process_response.raise_for_status()}")
            raise HTTPError
    except RequestException as r:
        logging.exception(f"Request exception error: {r}")
    except AttributeError as ae:
        logging.error(str(ae), exc_info=True)
    except TypeError as te:
        logging.error(f"TypeError: {te}", exc_info=True)
    except Exception as e:
        logging.exception(traceback.format_exc(str(e)))
    else:
        logging.info(f"{pr_body}")

    report_job_id = pr_body[0]["id"]

    success = False
    t0 = time.time()
    while not success:
        try:
            report_job_url = constant.FIND_REPORT_JOB_STATUS_URL.format(report_job_id=report_job_id)
            report_job_resp = requests_retry_session().get(url=report_job_url, headers=headers)
            logging.info(f"download report status - {report_job_resp.raise_for_status()}")
        except Timeout as rte:
            logging.exception(f"report job status response timed out: timeout error - {rte}", stack_info=True)
            try:
                report_job_resp = requests_retry_session().get(url=report_job_url, headers=headers)
            except Timeout:
                pass
        except (TypeError, Exception) as te:
            logging.exception(f"something happend - {te}", exc_info=True)
        else:
            if report_job_resp.status_code == 200 and report_job_resp.json()["status"] == "SUCCESS":
                report_job_time = round(((time.time() - t0) / 60), 4)
                logging.info(f"job info retrieved in {report_job_time} minutes and secs")
                rj_body = report_job_resp.json()
                rj_body["delivery"] = "SPECIFIC"
                rj_body["deliveryMethod"] = "PYTHON"
                report_job_status = rj_body["status"]
                logging.info(f"report job done processing and setting report job config delivery details {report_job_status}")
                success = True
            elif report_job_resp.json()["status"] in ("PENDING", "IN_PROGRESS") and not report_job_resp.json()["compressed"]:
                delay_ = time.time() - t0
                time.sleep(delay_)
                logging.info(f"delay retry for {delay_} seconds since report job status is in {report_job_resp.json()['status']} and file is not compressed")
            elif report_job_resp.json()["status"] in ("PENDING", "IN_PROGRESS") and report_job_resp.json()["compressed"]:
                delay = time.time() - t0
                time.sleep(delay)
                logging.info(f"delay retry for {delay} seconds for compressed zip export since data date range is large")
            elif report_job_resp.json()["status"] == "FAILURE":
                logging.error(f"report job failed due process successfully due to - {report_job_resp.json()['statusReason']} Exiting process...")
                sys.exit("report job status failed, exiting Dag/report api process....{rep}")
            else:
                time.sleep(30)
                logging.info(f"retrying getting report job status in 30 seconds - {report_job_resp.json()['status']}")

    job_id = rj_body["id"]
    logging.info(f"report job configuration completed and status is success, job {job_id} downloading report....")
    download_report_url = constant.DOWNLOAD_REPORT_URL.format(report_job_id=job_id)
    # TODO return download_report_url & have helper function to download report to output to csv with compressed or not

    # uploading raw report export to GCS bucket 
    logging.info("download raw report downloaded to container disk and then uploading to raw input bucket")
    url_file_path = download_url_file(download_url=download_report_url, headers=headers)
    skip_num = get_skip_row_num(url_file_path)
    raw_uri = f"gs://{extract_bucket}/{extract_path}/{url_file_path}"
    logging.info(f"url downloaded to container disk/local path - {url_file_path} lines skipped - {skip_num} and final input gcs_uri is {raw_uri}")

    gcs_io = gcs_auth_client()
    storage_client = gcs_io.gcs_client

    upload_file_from_disk(gcs_client=storage_client, bucket_name=extract_bucket, bucket_path=extract_path, bucket_filename=url_file_path, source_filename=url_file_path)
    # cleaning up unformatted csv report export

    logging.info(f"getting number of rows to skip as - {skip_num} and raw file uploaded to GCS")
    clean_uri = f"gs://{report_bucket}/{report_path}/{report_fn}"
    dt_values = (report_start, report_end)

    if args.dtype:
        dtype_mapping = args.dtype
        memory = True
    else:
        dtype_mapping = None
        memory = False

    if compressed:
        compression_type = "zip"
    else:
        compression_type = "infer"

    if filter_config:
        df_report = pd.read_csv(url_file_path, skiprows=skip_num, compression=compression_type, dtype=dtype_mapping, low_memory=memory)
        df_report[["extract_start_date", "extract_end_date"]] = pd.to_datetime(dt_values, infer_datetime_format=True)
    else:
        df_list = []
        logging.info("report not filtered so using low_memory to read and format")
        df_chunker = pd.read_csv(url_file_path, skiprows=skip_num, chunksize=25000, compression=compression_type, low_memory=memory, dtype=dtype_mapping)
        for df_chunk in df_chunker:
            df_chunk[["extract_start_date", "extract_end_date"]] = pd.to_datetime(dt_values, infer_datetime_format=True)
            df_list.append(df_chunk)
        df_report = pd.concat(df_list, ignore_index=True)

    logging.info(f"raw report extract at {raw_uri} and chunking out dataframe to add extract date columns to avoid k8 404 error on pod")
    logging.info(f"report dataframe formatting and cleansing finalized, {df_report.shape[0]} total rows fetched report extract date range")

    if clean_uri.endswith("csv"):
        clean_bucket = storage_client.get_bucket(report_bucket)
        chunk_size = 1024 * 1024 * 10  # 10 MB
        clean_blob = clean_bucket.blob(report_path + "/" + report_fn, chunk_size=chunk_size)
        clean_blob.upload_from_string(data=df_report.to_csv(index=False, encoding="utf-8", compression=compression_type), content_type="text/csv", timeout=300)
        logging.info("final extract file csv format and decompressed from zip file with chunk")
    elif clean_uri.endswith("parquet"):
        logging.info(f"output file cast to string type for {clean_uri.split('.')[-1]} file handling")
        df_report = df_report.astype("string")
        gcs_io.upload_file_from_object(gcs_uri=clean_uri, object_to_upload=df_report, index=False, \
            pandas_kwargs={"engine": "pyarrow", "compression": "snappy", "allow_truncated_timestamps": True})
    else:
        file_type = clean_uri.split(".")[-1]
        logging.info(f"output file using dtypes from convert_dtypes() converted to dtypes for {file_type} file handling")
        df_report = df_report.convert_dtypes()
        gcs_io.upload_file_from_object(gcs_uri=clean_uri, object_to_upload=df_report, index=False, default_file_type=file_type)

    logging.info(f"assigned extract start & end dates {dt_values} to export and outputting final report to gcs path - {clean_uri}")

    logging.info("checking to see if reports in workspace need to be removed")
    cleanup_old_reports(workspace_id=workspace_id, headers=headers)


def datorama_report_parse_args():

    parser = argparse.ArgumentParser(description="Datorama Report API arguments to use to setup report configuration and generate flat file")

    parser.add_argument("--extract_output_bucket_name", type=str, required=True, default="cp-saa-media-dev_datorama",
                        help="GCS Bucket name to of where to store datorama raw report extract flat file for staging and loading. File extension should be CSV file format")

    parser.add_argument("--extract_output_path", type=str, required=False, default=None,
                        help="GCS bucket path or local folder path where to write the raw datorama report extract to be used in main ingestion logic")

    parser.add_argument("--extract_output_filename", type=str, required=True, default=None,
                        help="GCS bucket fileame or local filename to write raw report extract CSV file to")

    parser.add_argument("--report_output_bucket_name", type=str, required=True,
                        help="GCS bucket name to output finalized datorama report flat file to when done processing and cleaning in after ingestion logic complete")

    parser.add_argument("--report_output_path", type=str, required=False, default=None,
                        help="local or GCS bucket path where to write the Datorama finalized report flat file")

    parser.add_argument("--report_output_filename", type=str, required=True,
                        help="Datorama finalized report filename to write to in GCS after data is cleaned and parsed. Parquet file format most stable or CSV as 2nd option")

    parser.add_argument("--report_start_date", type=str, required=True,
                        help="Start date to specify the beginning of the time frame that the report will cover in YYYY-MM-DD format.")

    parser.add_argument("--report_end_date", type=str, required=True,
                        help="End date specified in YYYY-MM-DD format to designate end of the time period the report will cover.")

    parser.add_argument("--workspace_id", type=int, required=True,
                        help="the start date to look for at range of historical look for STAT data in YYYY-MM-DD format.")

    parser.add_argument("--measurements", type=json.loads, required=True,
                        help="json string list of numerical fields needed to pass into the report configuration. \
                            System name conventions are required to pass into container, since they will have displayName conventions mapped in report config.")

    parser.add_argument("--dimensions", type=json.loads, required=True,
                        help="json string list of categorical fields needed for report configuration. System Names are required \
                            to successfully create the report through the API. The first field passed into the list of Dimensions will be inferred \
                            as the sort by AKA order by field in descending order. If there is a specific field required to be used as the sort by/order \
                            field, ensure that it is passed as the first field, specifically at index position 0 in the list of Dimensions. \
                            Master list of system and display convention names for measurements and dimesnsions can be fetched from API calls \
                            found in constant.LIST_DIMENSIONS_URL by workspace_id")

    parser.add_argument("--filter_config", type=json.loads, required=False,
                        help="json string dictionary specifiying value(s) to filter by Dimension field(s) passed to configuration. \
                            Each dict key:value pair in config references to a dimension display/system name and a list of value(s) as strings \
                            to filter report by. The `stringDimensionFiltersOperator` can either be `AND` or `OR`. Fields passed into filter_config \
                            argument can either take the form of displayName or systemName format of Dimension(s), container infers correct naming convention in report configuration. \
                            Example config as such: `{*Market [Market Classification]': ['Thailand', 'Vietnam'], \
                            '*Obj by ACPL [Campaign Objective Classification]': ['NA', 'A'], 'stringDimensionFiltersOperator': 'OR'}`")

    parser.add_argument("--compressed", action="store_true", required=False,
                        help="Toggle flag acting as a boolean to pass into arguments determining whether report export should be compressed as zip file. \
                            This argument is strongly recommended for reports that cover large date ranges and/or have large quantity of dimensions/measurements fields queried \
                            If left out of arguments, extract_output_filenmae will be csv flat file while report_output_filename will follow the file extension specified.")

    parser.add_argument("--fetch_updates", action="store_true", required=False,
                        help="Passing this flag, will cause it behave like a boolean. If passed in container arguments it will render as True \
                            however if omitted from arguments, will render as False. Toggling this arugment flag indicates whether the report dates filter \
                            applies in data dates or is based on when data was updated ")

    #TODO partition_report_by in next container release version to minimize # of reports created and increased download speed of reports and jobs
    # parser.add_argument("--partition_report_by", default=str(), type=str, choices=["Day", "Week", "Month", "BiWeek", "Quarter", "Year", ""],
    #                     help="Argument specified as a string time period from the choices above, splits the report date_range by time period declared. \
    #                         For ex, report_id with start & end dates as 2020-10-01 to 2020-10-31 with --partition_report_by as `Week` will create 4 jobs \
    #                         for each week of the month. If omitted from container args, report will be created without time period partition as single flat file")

    parser.add_argument("--dtype", type=json.loads, required=False, help="A JSON dictionary of (column: type) pairs to cast columns to")


    args = parser.parse_args()
    logging.info(f"Input arguments: {args}")
    return args


if __name__ == "__main__":
    dato_args = datorama_report_parse_args()
    main_datorama_report(args=dato_args)
