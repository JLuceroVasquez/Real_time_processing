import pprint
import requests
import json
import mysql.connector
import pymysql

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

def fetch_weather_data():
    try:
        # Realiza la solicitud a la API de clima
        api_url = 'https://api.openweathermap.org/data/2.5/weather?lat=-12.04&lon=-77.02&appid=0665a8eaf0ce7b6a810d6811dd520025'
        response = requests.get(api_url)
        weather_data = response.json()
        return weather_data
    except Exception as e:
        print(f"Error al obtener datos del clima: {str(e)}")
        return None

def print_json(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_weather_task')  # Obtener el JSON de la tarea anterior
    if json_data:
        pprint.pprint(json_data)  # Imprimir el JSON utilizando pprint
    else:
        print("No se pudo obtener el JSON de datos del clima")

def json_serialization(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_weather_task')  # Obtener el JSON de la tarea anterior
    if json_data:
        producer_config = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'airflow'
        }
        producer = Producer(producer_config)
        json_message = json.dumps(json_data)
        producer.produce('airflow-kafka-spark', value=json_message)
        producer.flush()
    else:
        print("No se pudo obtener el JSON de datos del clima")

def transform_with_spark(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_weather_task')  # Obtener el JSON de la tarea anterior
    if json_data:
        try:
            spark = SparkSession.builder.appName("WeatherDataTransformation").getOrCreate()
            sc = spark.sparkContext

            # Convertir JSON a RDD y luego a DataFrame
            rdd = sc.parallelize([json_data])
            df = spark.read.json(rdd)

            # Transformar los datos
            df_transformed = df.select(
                from_unixtime(col("dt")).alias("timestamp"),
                (col("main.temp") - 273.15).alias("temperature"),
                col("main.humidity").alias("humidity"),
                col("weather")[0].getField("description").alias("weather_description"),
                col("name").alias("city"),
                col("sys.country").alias("country")
            )

            # Convertir DataFrame a JSON y pasarlo a la siguiente tarea
            transformed_data = df_transformed.toJSON().collect()
            transformed_json = json.loads(transformed_data[0])
            ti.xcom_push(key='transformed_data', value=transformed_json)
        except Exception as e:
            print(f"Error al transformar datos con Spark: {str(e)}")
    else:
        print("No se pudo obtener el JSON de datos del clima")

def insert_into_mysql(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='transform_with_spark')  # Obtener el JSON de la tarea anterior
    if json_data:
        try:
            # Conectar a la base de datos MySQL
            conn = BaseHook.get_connection('mysql_root')
            connection = pymysql.connect(
                host=conn.host,
                user=conn.login,
                password=conn.password,
                database='clima_data'
            )
            cursor = connection.cursor()

            # Extraer datos del JSON
            timestamp = json_data['timestamp']
            temperature = json_data['temperature']
            humidity = json_data['humidity']
            weather_description = json_data['weather_description']
            city = json_data['city']
            country = json_data['country']

            # Insertar datos en la tabla MySQL
            insert_query = """
            INSERT INTO weather_data (timestamp, temperature, humidity, weather_description, city, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (timestamp, temperature, humidity, weather_description, city, country))
            connection.commit()
            
            print("Datos insertados correctamente.")
            
            # Cerrar la conexión
            cursor.close()
            connection.close()
        except mysql.connector.Error as err:
            print(f"Error al insertar en MySQL: {err}")
    else:
        print("No se pudo obtener el JSON de datos del clima")

dag = DAG('mi_dag_api', 
          schedule_interval=timedelta(seconds=5),
          start_date=datetime(2023, 10, 6))

task1 = PythonOperator(
    task_id='fetch_weather_task',
    python_callable=fetch_weather_data,
    dag=dag,
    provide_context=True,
)

task2 = PythonOperator(
    task_id='get_json_from_task1',
    python_callable=print_json,
    dag=dag,
    provide_context=True,
)

task3 = PythonOperator(
    task_id='run_kafka_producer',
    python_callable=json_serialization,
    dag=dag,
    provide_context=True,
)

task4 = PythonOperator(
    task_id='transform_with_spark',
    python_callable=transform_with_spark,
    dag=dag,
    provide_context=True,
)

task5 = PythonOperator(
    task_id='insert_into_mysql',
    python_callable=insert_into_mysql,
    dag=dag,
    provide_context=True,
)

task1 >> task2 >> task3 >> task4 >> task5
