import os
from datetime import datetime, date
from io import StringIO
from os import environ as env, getenv
from celery import group
from . import app as celery_app


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





# @celery_app.task(bind=True)
# def process_latest_inventory(self):
#     src_inv_bkt = env['SOURCE_INVENTORY_BUCKET']
#     src_data_bkt = env['SOURCE_DATA_BUCKET']
#     tsk_id = self.request.id

#     logger.info(
#         f'Started task {tsk_id} to fetch inventory manifest for daily_health_data_transfer from SOURCE_DATA_BUCKET '
#         f'[{src_data_bkt}]')

#     if 'OVERRIDE_S3_ENDPOINT' in os.environ:
#         inventory_bucket = src_inv_bkt
#         inventory_id = env['MANIFEST_ITEMS_PATH']
#         logger.debug(f"Override S3 Endpoint detected, Using bucket:[{inventory_bucket}] as inventory bucket")
#     else:
#         logger.info(f"Looking up inventories configured on bucket = [{src_data_bkt}]")
#         inventory_cfg = Inventory.latest_inventory_configured(bucket=src_data_bkt)
#         inventory_id = inventory_cfg[0]
#         inventory_bucket = inventory_cfg[1]
#     inventory = Inventory(bucket=inventory_bucket)
#     inventory_items = inventory.fetch_manifest_items(f'{src_data_bkt}/{inventory_id}')
#     if len(inventory_items) > 0:
#         partial_date_msg = date_filter_partial_log_msg()
#         logger.info(
#             f'Started daily health data transfer process {partial_date_msg}.')
#     try:
#         logger.info(f'inventory details : {inventory_items}')
#         group(process_workload.s(gz_name=item, txn_id=tsk_id) for item in inventory_items)()
#     except Exception as err:
#         logger.critical(f'Failed to schedule data processing workloads for daily_health_data_transfer task [{tsk_id}]',
#                         exc_info=err)
#         raise WorkSchedulingException(f'Failed to schedule data processing workloads for task [{tsk_id}]')
