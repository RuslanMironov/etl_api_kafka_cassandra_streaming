import time
import uuid
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "polly_wanna_cracker",
    "start_date": datetime.now()
}

def get_data() -> dict:
    import requests

    result = requests.get("https://randomuser.me/api/")
    result = result.json()["results"][0]

    return result

def data_formatted(result: dict) -> dict:
    data = {}
    location = result["location"]
    data["id"] = str(uuid.uuid4())
    data["first_name"] = result["name"]["first"]
    data["last_name"] = result["name"]["last"]
    data["gender"] = result["gender"]
    data["address"] = (
        f"{str(location['street']['number'])} {location['street']['name']}, "
        f"{location['city']}, {location['state']}, {location['country']}"        
        )
    data["post_code"] = location["postcode"]
    data["email"] = result["email"]
    data["username"] = result["login"]["username"]
    data["dob"] = result["dob"]["date"]
    data["reg_date"] = result["registered"]["date"]
    data["phone"] = result["phone"]
    data["picture"] = result["picture"]["medium"]

    return data

def stream_data() -> None:
    import json
    import logging

    from kafka import KafkaProducer

    result = get_data()
    result_data = data_formatted(result)

    time_limit = 666

    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    topic_name = "creating_user_topic"
    current_time = time.time()
    while True:
        if time.time() > current_time + time_limit:
            break
        try:
            result = get_data()
            result_data = data_formatted(result)
            producer.send(topic_name, json.dumps(result_data).encode("utf-8"))
        except Exception as e:
            logging.error(f"Error kafka send: {e}")

with DAG(
    "user_automation",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id="stream_data",
        python_callable=stream_data
    )

    streaming_task







    

