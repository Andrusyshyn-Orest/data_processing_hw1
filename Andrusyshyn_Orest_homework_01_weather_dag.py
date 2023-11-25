from datetime import datetime
import json
from airflow import DAG
from airflow.models import Variable
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

cities = {
    "Lviv":      {"lat": "49.841952",  "lon": "24.0315921"},
    "Kyiv":      {"lat": "50.4500336", "lon": "30.5241361"},
    "Kharkiv":   {"lat": "49.9923181", "lon": "36.2310146"},
    "Odesa":     {"lat": "46.4843023", "lon": "30.7322878"},
    "Zhmerynka": {"lat": "49.0354593", "lon": "28.1147317"}
}

def get_historic_dt(date_string):
    date_format = "%Y%m%dT%H%M%S"
    datetime_object = datetime.strptime(date_string, date_format)
    timestamp = int(datetime.timestamp(datetime_object))
    return timestamp

def _process_weather(**kwargs):
    info = kwargs["ti"].xcom_pull(kwargs["extract_data_task_id"])
    timestamp = info["data"][0]["dt"]
    temp = info["data"][0]["temp"]
    hum = info["data"][0]["humidity"]
    clouds = info["data"][0]["clouds"]
    wind_speed = info["data"][0]["wind_speed"]
    return kwargs["city"], timestamp, temp, hum, clouds, wind_speed


with DAG(
            dag_id="Andrusyshyn_Orest_homework_01_weather",
            schedule_interval="@daily",
            start_date=datetime(2023, 11, 20),
            catchup=True,
            user_defined_macros={
                    'get_historic_dt': get_historic_dt
            }
        ) as dag:
    db_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="airflow_conn_for_weather",
        sql="""
        CREATE TABLE IF NOT EXISTS measures
        (
            city        VARCHAR(255),
            timestamp   TIMESTAMP,
            temp        FLOAT8,
            hum         INTEGER,
            clouds      INTEGER,
            wind_speed  FLOAT8
        );"""
    )

    for city, coordinates in cities.items():
        check_api = HttpSensor(
            task_id=f"check_api_{city}",
            http_conn_id="airflow_http_weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            request_params={"appid": Variable.get("WEATHER_API_KEY"),
                            "lat": coordinates["lat"],
                            "lon": coordinates["lon"],
                            "dt":"{{ get_historic_dt(ts_nodash) }}"})

        extract_data = SimpleHttpOperator(
            task_id=f"extract_data_{city}",
            http_conn_id="airflow_http_weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            data={"appid": Variable.get("WEATHER_API_KEY"),
                "lat": coordinates["lat"],
                "lon": coordinates["lon"],
                "dt": "{{ get_historic_dt(ts_nodash) }}"},
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True
            )

        process_data = PythonOperator(
            task_id=f"process_data_{city}",
            python_callable=_process_weather,
            provide_context=True,
            op_kwargs={"city": city, "extract_data_task_id": f"extract_data_{city}"}
        )

        inject_data = SqliteOperator(
            task_id=f"inject_data_{city}",
            sqlite_conn_id="airflow_conn_for_weather",
            sql="""
            INSERT INTO measures (city, timestamp, temp, hum, clouds, wind_speed) VALUES
                (
                    '{{ti.xcom_pull(task_ids='process_data_%s')[0]}}',
                    {{ti.xcom_pull(task_ids='process_data_%s')[1]}},
                    {{ti.xcom_pull(task_ids='process_data_%s')[2]}},
                    {{ti.xcom_pull(task_ids='process_data_%s')[3]}},
                    {{ti.xcom_pull(task_ids='process_data_%s')[4]}},
                    {{ti.xcom_pull(task_ids='process_data_%s')[5]}}
                );
            """ % (city, city, city, city, city, city)
        )
        db_create >> check_api >> extract_data >> process_data >> inject_data
