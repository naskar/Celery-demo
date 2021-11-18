import os
from datetime import datetime, date
from io import StringIO
from os import environ as env, getenv

# import awswrangler as wr
# import boto3
# import pandas as pd
# from botocore.exceptions import ClientError
from celery import group

# from common.errors import WorkSchedulingException
# from common.s3.operations import Inventory
# from common.utils.datetime import from_human_date
# from common.utils.inventory import filter_by_dates
# from common.utils.locks import DistributedLock
from . import app as celery_app
# from . import logger
# from .daily_health_data_transfer import migrate_daily_health_data
# from .helpers import performance_monitoring

NAMED_COLUMNS = ['src', 'paytype', 'datatype', 'cats', 'pid', 'start_end']
ERROR_FILE_HEADERS = ['bucket', 'path', 'payload', 'dt', 'status']
DAILY_DATA_TRANSFER = 'DailyDataTransfer'


# @celery_app.task(bind=True)
# def kickoff_scheduled(self):
#     tsk_id = self.request.id
#     dist_lock = DistributedLock(name=DAILY_DATA_TRANSFER)
#     if dist_lock.lock_it(3):
#         logger.info(f'Starting scheduled daily health data transfer job with JobId=[{tsk_id}]...')
#         try:
#             process_latest_inventory.delay()
#         except Exception as error:
#             logger.error(
#                 f'Error while starting the scheduled daily health data transfer job with task id {tsk_id}, Exception '
#                 f'details {error}')
#             dist_lock.unlock_it()
#             raise
#     else:
#         logger.warn(
#             f"Failed to acquire processing lock. Skipping scheduled daily health data transfer job id [{tsk_id}]...")


@celery_app.task(bind=True)
def kickoff_startup(self):
    print("soumik")
    # tsk_id = self.request.id
    # logger.info(f"Starting new daily health data transfer job with JobId=[{tsk_id}]...")
    # dist_lock = DistributedLock(name=DAILY_DATA_TRANSFER)
    # if dist_lock.lock_it(3):
    #     logger.info(f'Acquired lock, invoking startup daily_health_data_transfer job id [{tsk_id}]...')
    #     try:
    #         process_latest_inventory.delay()
    #     except Exception as error:
    #         logger.error(f'Failed to start the daily_health_data_transfer job id {tsk_id}')
    #         dist_lock.unlock_it()
    #         raise
    # else:
    #     logger.warn(f"Failed to acquire startup lock, Skipping startup daily_health_data_transfer job id [{tsk_id}]...")


def date_filter_partial_log_msg():
    _start_dt_param = getenv('FILTER_START_DATE', '')
    _end_dt_param = getenv('FILTER_END_DATE', '')
    partial_date_msg = ''
    if len(_start_dt_param) > 0 and len(_end_dt_param) > 0:
        _start_dt = datetime.strftime(from_human_date(_start_dt_param), format="%y/%m/%d %H:%M:%S")
        _end_dt = datetime.strftime(from_human_date(_end_dt_param, datetime.max), format="%y/%m/%d %H:%M:%S")
        partial_date_msg = f'with date filter between [{_start_dt} and {_end_dt}]'
    elif len(_start_dt_param) > 0 >= len(_end_dt_param):
        _start_dt = datetime.strftime(from_human_date(_start_dt_param), format="%y/%m/%d %H:%M:%S")
        partial_date_msg = f'with date filter from [{_start_dt}]'
    elif len(_start_dt_param) <= 0 < len(_end_dt_param):
        _end_dt = datetime.strftime(from_human_date(_end_dt_param, datetime.max), format="%y/%m/%d %H:%M:%S")
        partial_date_msg = f'with date filter until [{_end_dt}]'
    elif len(_start_dt_param) == 0 == len(_end_dt_param):
        partial_date_msg = ''
    return partial_date_msg


@celery_app.task(bind=True)
def process_latest_inventory(self):
    src_inv_bkt = env['SOURCE_INVENTORY_BUCKET']
    src_data_bkt = env['SOURCE_DATA_BUCKET']
    tsk_id = self.request.id

    logger.info(
        f'Started task {tsk_id} to fetch inventory manifest for daily_health_data_transfer from SOURCE_DATA_BUCKET '
        f'[{src_data_bkt}]')

    if 'OVERRIDE_S3_ENDPOINT' in os.environ:
        inventory_bucket = src_inv_bkt
        inventory_id = env['MANIFEST_ITEMS_PATH']
        logger.debug(f"Override S3 Endpoint detected, Using bucket:[{inventory_bucket}] as inventory bucket")
    else:
        logger.info(f"Looking up inventories configured on bucket = [{src_data_bkt}]")
        inventory_cfg = Inventory.latest_inventory_configured(bucket=src_data_bkt)
        inventory_id = inventory_cfg[0]
        inventory_bucket = inventory_cfg[1]
    inventory = Inventory(bucket=inventory_bucket)
    inventory_items = inventory.fetch_manifest_items(f'{src_data_bkt}/{inventory_id}')
    if len(inventory_items) > 0:
        partial_date_msg = date_filter_partial_log_msg()
        logger.info(
            f'Started daily health data transfer process {partial_date_msg}.')
    try:
        logger.info(f'inventory details : {inventory_items}')
        group(process_workload.s(gz_name=item, txn_id=tsk_id) for item in inventory_items)()
    except Exception as err:
        logger.critical(f'Failed to schedule data processing workloads for daily_health_data_transfer task [{tsk_id}]',
                        exc_info=err)
        raise WorkSchedulingException(f'Failed to schedule data processing workloads for task [{tsk_id}]')


# @celery_app.task(bind=True)
# def process_workload(self, gz_name: str, txn_id):
#     inventory_bucket_name = env['SOURCE_INVENTORY_BUCKET']
#     header_list = ['bucket', 'path', 'size', 'dt']
#     try:
#         inventory_df = wr.s3.read_csv(f's3://{inventory_bucket_name}/{gz_name}',
#                                       names=header_list,
#                                       usecols=['path', 'dt'], parse_dates=['dt'],
#                                       infer_datetime_format=True)
#     except Exception as err_ex:
#         logger.error(f'Failed to process the file {gz_name} due to ........  {err_ex.__cause__}', exc_info=err_ex)
#         raise RuntimeError(f'{txn_id} Failed to complete gz parsing process due to {err_ex}')

#     zone_prefix = getenv('ZONE_PREFIX', '')
#     if zone_prefix is not None:
#         inventory_df = inventory_df[inventory_df['path'].str.contains(zone_prefix)]
#     filter_start_date = getenv('FILTER_START_DATE', '')
#     filter_end_date = getenv('FILTER_END_DATE', '')
#     filter_time_window = getenv('TIME_WINDOW', '')
#     try:
#         filtered_df: pd.DataFrame = filter_by_dates(filter_start_date, filter_end_date, inventory_df,
#                                                     filter_time_window)
#         logger.info(f'Inventory filtered data by date : {filtered_df}')
#         if filtered_df.empty:
#             logger.info("Found no data within the mentioned date range")
#         migrate_daily_health_data_to_gcs.apply_async(args=[filtered_df.to_json(), txn_id])
#     except Exception as err_ex:
#         logger.error(f'Task failed due to {err_ex.__cause__}', exc_info=err_ex)


# @celery_app.task(bind=True)
# def migrate_daily_health_data_to_gcs(self, filtered_df, txn_id):
#     try:
#         filtered_df = pd.read_json(StringIO(filtered_df))
#         success_data = {'bucket': [], 'path': [], 'payload': [], 'dt': [], 'status': []}
#         error_data = {'bucket': [], 'path': [], 'payload': [], 'dt': [], 'status': []}
#         inventory = Inventory(bucket=env['SOURCE_DATA_BUCKET'])
#         for index, row in filtered_df.iterrows():
#             try:
#                 if not row['path'] in success_data['path'] and not row['path'] in error_data['path']:
#                     if row['path'].endswith('.link'):
#                         start = datetime.now()
#                         payload_json_path = inventory.fetch_link_items(row['path'])
#                         status, skip_status, size_of_payload_file = migrate_daily_health_data(
#                             bucket_name=env['GCS_TARGET_BUCKET'],
#                             payload_json_path=payload_json_path)
#                         end = datetime.now()
#                         performance_monitoring(start, end, payload_json_path)
#                     else:
#                         start = datetime.now()
#                         payload_json_path = row['path']
#                         status, skip_status, size_of_payload_file = migrate_daily_health_data(
#                             bucket_name=env['GCS_TARGET_BUCKET'],
#                             payload_json_path=payload_json_path)
#                         end = datetime.now()
#                         performance_monitoring(start, end, payload_json_path)
#                     now = datetime.now()
#                     dt_string = now.strftime("%d-%m-%YT%H:%M:%S.000Z")
#                     if status:
#                         success_data['bucket'].append(env['SOURCE_DATA_BUCKET'])
#                         success_data['path'].append(payload_json_path)
#                         success_data['payload'].append(size_of_payload_file)
#                         success_data['dt'].append(dt_string)
#                         success_data['status'].append('Success')
#                         logger.info(f'Transfer has been completed without error - {payload_json_path}')
#                     else:
#                         error_data['bucket'].append(env['SOURCE_DATA_BUCKET'])
#                         error_data['path'].append(payload_json_path)
#                         error_data['payload'].append(size_of_payload_file)
#                         error_data['dt'].append(dt_string)
#                         if skip_status == 'Skipped':
#                             error_data['status'].append('skipped')
#                         else:
#                             error_data['status'].append('Failed')
#                         logger.warn(f'Transfer has been completed with error - {payload_json_path}')
#             except ClientError as ex:
#                 if ex.response['Error']['Code'] == 'NoSuchKey':
#                     logger.error(
#                         f'File is not available in Inventory, generate the link file - {row["path"]} and try it again')
#             except Exception as err:
#                 logger.error(f'Task Failed due to failure while fetching .link file in Inventory',
#                              extra={'txn_id': f'{txn_id}'}, exc_info=err)
#         save_results(success_data, error_data, txn_id)

#     except Exception as err:
#         logger.error(f"Error while saving data to s3", exc_info=err)


# @celery_app.task(bind=True)
# def save_results(self, success_data, error_data, txn_id, sess: boto3.session.Session = None):
#     try:
#         date_path = date.today()
#         error_pq_path = f's3://{env["DEST_DATA_BUCKET"]}/logs/dailyTransfers/{date_path}/{env["ERROR_LOG"]}'
#         success_pq_path = f's3://{env["DEST_DATA_BUCKET"]}/logs/dailyTransfers/{date_path}/{env["SUCCESS_LOG"]}'
#         success_log = pd.DataFrame(success_data)
#         error_log = pd.DataFrame(error_data)
#         if not success_log.empty:
#             wr.s3.to_csv(df=success_log, path=success_pq_path, boto3_session=sess, dataset=True, mode='append')
#             logger.info("success logs saved successfully")
#         if not error_log.empty:
#             wr.s3.to_csv(df=error_log, path=error_pq_path, boto3_session=sess, dataset=True, mode='append')
#             logger.info("error logs saved successfully")
#     except Exception as err:
#         logger.critical(f'Task Failed while creating the log file',
#                         extra={'txn_id': f'{txn_id}'}, exc_info=err)
#         raise
