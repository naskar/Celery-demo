from . import app as celery_app
from .loggers import full_logger
import pandas as pd
from celery import group, chord

LOG = full_logger("demo_task")

@celery_app.task(bind=True)
def kickoff_startup(self):
    task_id = self.request.id
    dataFrame = pd.read_csv("/Users/soumiknaskar/Documents/Celery_demo/Celery-demo/demo_task/data.csv")
    group(process_workload.s(file_name=item, txn_id=task_id) for item in dataFrame['FileName'])()
    # chord((process_workload.s(file_name=item, txn_id=task_id) for item in dataFrame['FileName']), afterComplete.s())()
    print("I need to be executed after processing the dataframe")

@celery_app.task(bind=True)
def process_workload(self, file_name: str, txn_id):
    try:
        print(f"This file - {file_name} is picked up by the following worker id - {txn_id}")
    except Exception as err_ex:
        LOG.error(f'Task failed due to {err_ex.__cause__}', exc_info=err_ex)

# @celery_app.task(bind=True)
# def afterComplete():
#     try:
#         print("I need to be executed after processing the dataframe")
#     except Exception as err_ex:
#         LOG.error(f'Task failed due to {err_ex.__cause__}', exc_info=err_ex)


