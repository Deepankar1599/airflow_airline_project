from airflow.sdk import dag, task
import os
import sys 
import requests
from datetime import datetime,timedelta
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

sys.path.append(os.path.join(os.path.dirname(__file__),".."))


from utils.bronze_layer import BronzeLayer

# os.environ["AIRFLOW_CONN_DBT_CLOUD_DEFAULT"] = "//nj024.us1.dbt.com"

ACCOUNT_ID = "70506183144876"  # Found in your dbt Cloud URL right after /deploy/
JOB_ID = "70506183139983" 




@dag(
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False
)

def aws_project():

    @task.python(retries=3,retry_delay=timedelta(seconds=5))
    def extract_load(ds:str=None):
        #fetching data from API and loading it into S3 bucket

        urls=["https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv",
              "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/passengers.csv",
              "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/airports.csv"]

        obj=BronzeLayer()

        # folder_name=datetime.now().strftime("%Y-%m-%d")

        for url in urls:
            fetched_data=obj.ingest_data_api(url)
            obj.put_data_s3("awsairflowproject",f"bronze/{ds}/{url.split('/')[-1]}",fetched_data)

    trigger_databricks = DatabricksRunNowOperator(
                task_id='trigger_databricks_pipeline',
                databricks_conn_id='databricks_default',  # Matches the env variable name
                job_id=90107865620121                       # Replace with your actual Databricks Job ID
                
    )

    
    # trigger_dbt_via_api = HttpOperator(
    # task_id="trigger_dbt_via_api",
    # http_conn_id="dbt_cloud_default", 
    # endpoint=f"/api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/",
    # method="POST",
    # headers={
    #     # 2. Fixed the string format to properly pass the authentication header
    #     "Authorization": f"Token {DBT_CLOUD_API_TOKEN}", 
    #     "Content-Type": "application/json"
    # },
    # data=json.dumps({"cause": "Triggered by Airflow Workaround"}),
    # )

    def trigger_dbt_job_via_python(**context):

        try:
                api_token = Variable.get("dbt_cloud_api_token").strip()
        except KeyError:
                raise ValueError(
                    "❌ Critical Error: 'dbt_cloud_api_token' is not set in Airflow Admin -> Variables UI!"
                )


        url = f"https://nj024.us1.dbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/"
        headers = {
            "Authorization": f"Bearer  {api_token}",
            "Content-Type": "application/json"
        }
        payload = {"cause": "Triggered via Airflow PythonOperator"}
        
        print(f"🚀 Sending direct POST request to: {url}")
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code in (200,201):
            run_details = response.json()
            run_id = run_details["data"]["id"]
            print(f"✅ Success! Job triggered. Run ID: {run_id}")
            return run_id
        else:
            raise Exception(f"❌ Failed with Status Code {response.status_code}: {response.text}")

    

    extract_load_task = extract_load()
    trigger_dbt_via_api = PythonOperator(
        task_id="trigger_job",
        python_callable=trigger_dbt_job_via_python,
    )
    extract_load_task >> trigger_databricks>>trigger_dbt_via_api

s3_dag=aws_project()