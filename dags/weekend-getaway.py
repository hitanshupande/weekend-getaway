from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import timedelta
import datetime
import json
import csv
import requests
import boto3
import time
import os



###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Weekend Getaway"
file_path = "/usr/local/spark/resources/data/airflow.cfg"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"



###############################################
# Establish s3 session with credentials
###############################################
session = boto3.Session()
credentials = session.get_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key

s3 = boto3.resource('s3',
    aws_access_key_id= access_key,
    aws_secret_access_key= secret_key)

###############################################
# API Parameters
###############################################
headers = {
            'x-rapidapi-key': Variable.get("x-rapidapi-key"),
            'x-rapidapi-host': "api.skypicker.com",
            'content-type': "application/x-www-form-urlencoded"
        }

#Return todays date as String
def todays_date():
    now = datetime.datetime.now()
    temp = now.strftime("%d")+'-'+now.strftime("%m")+'-'+now.strftime("%Y")
    return temp

date_today = todays_date()

#Calculate the next weekend dates
def next_weekday(d, weekday):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
    temp = d + timedelta(days_ahead)
    return temp


def download_quotes():
    #Calculate list of departure and return dates for next 4 weekends
    d = datetime.datetime.now() # Date format
    
    print("converted d ->", d)
    
    next_1_friday = next_weekday(d, 4)
    next_1_sunday = next_weekday(next_1_friday, 6)
    next_2_friday = next_weekday(next_1_sunday, 4)
    next_2_sunday = next_weekday(next_1_sunday, 6)
    next_3_friday = next_weekday(next_2_sunday, 4)
    next_3_sunday = next_weekday(next_2_sunday, 6)
    next_4_friday = next_weekday(next_3_sunday, 4)
    next_4_sunday = next_weekday(next_3_sunday, 6)
    
    next_1_friday_str = next_1_friday.strftime("%d")+'-'+next_1_friday.strftime("%m")+'-'+next_1_friday.strftime("%Y")
    next_1_sunday_str = next_1_sunday.strftime("%d")+'-'+next_1_sunday.strftime("%m")+'-'+next_1_sunday.strftime("%Y")
    next_2_friday_str = next_2_friday.strftime("%d")+'-'+next_2_friday.strftime("%m")+'-'+next_2_friday.strftime("%Y")
    next_2_sunday_str = next_2_sunday.strftime("%d")+'-'+next_2_sunday.strftime("%m")+'-'+next_2_sunday.strftime("%Y")
    next_3_friday_str = next_3_friday.strftime("%d")+'-'+next_3_friday.strftime("%m")+'-'+next_3_friday.strftime("%Y")
    next_3_sunday_str = next_3_sunday.strftime("%d")+'-'+next_3_sunday.strftime("%m")+'-'+next_3_sunday.strftime("%Y")
    next_4_friday_str = next_4_friday.strftime("%d")+'-'+next_4_friday.strftime("%m")+'-'+next_4_friday.strftime("%Y")
    next_4_sunday_str = next_4_sunday.strftime("%d")+'-'+next_4_sunday.strftime("%m")+'-'+next_4_sunday.strftime("%Y")


    #Read all airport combinations maintained in airports.csv
    path_to_json = '/usr/local/spark/resources/quotes/'+str(date_today)+'/'

    if not os.path.isdir(path_to_json):
        os.makedirs(path_to_json)
        with open('/usr/local/spark/resources/data/airports.csv') as airports:
            reader = csv.DictReader(airports, delimiter=';')
            for row in reader:
                home_airport = row['home_airport']
                destination_airport = row['destination_airport'].split(' ')
                for x in destination_airport:
                    try:
                        x = x.upper()
                        url1 = 'https://api.skypicker.com/flights?' + 'flyFrom=' + home_airport + '&to=' + x + '&dateFrom=' + next_1_friday_str.replace("-","/") + '&dateTo=' + next_1_sunday_str.replace("-","/") + '&partner=picky&v=3&sort=price&limit=1&asc=1'
                        print(url1)
                        url2 = 'https://api.skypicker.com/flights?' + 'flyFrom=' + home_airport + '&to=' + x + '&dateFrom=' + next_2_friday_str.replace("-","/") + '&dateTo=' + next_2_sunday_str.replace("-","/") + '&partner=picky&v=3&sort=price&limit=1&asc=1'
                        url3 = 'https://api.skypicker.com/flights?' + 'flyFrom=' + home_airport + '&to=' + x + '&dateFrom=' + next_3_friday_str.replace("-","/") + '&dateTo=' + next_3_sunday_str.replace("-","/") + '&partner=picky&v=3&sort=price&limit=1&asc=1'
                        url4 = 'https://api.skypicker.com/flights?' + 'flyFrom=' + home_airport + '&to=' + x + '&dateFrom=' + next_4_friday_str.replace("-","/") + '&dateTo=' + next_4_sunday_str.replace("-","/") + '&partner=picky&v=3&sort=price&limit=1&asc=1'
                        response1 = requests.request("GET", url1, headers=headers).json()
                        time.sleep(1)
                        response2 = requests.request("GET", url2, headers=headers).json()
                        time.sleep(1)
                        response3 = requests.request("GET", url3, headers=headers).json()
                        time.sleep(1)
                        response4 = requests.request("GET", url4, headers=headers).json()
                        time.sleep(1)
                        #Dump json file in file storage
                        file_name1 = path_to_json+home_airport+'_'+x+'_'+'1.json'
                        file_name2 = path_to_json+home_airport+'_'+x+'_'+'2.json'
                        file_name3 = path_to_json+home_airport+'_'+x+'_'+'3.json'
                        file_name4 = path_to_json+home_airport+'_'+x+'_'+'4.json'
                        with open(file_name1, 'a') as outfile1:
                            json.dump(response1, outfile1)
                            print("Done with: " + file_name1)
                        with open(file_name2, 'a') as outfile2:
                            json.dump(response2, outfile2)
                            print("Done with: " + file_name2)
                        with open(file_name3, 'a') as outfile3:
                            json.dump(response3, outfile3)
                            print("Done with: " + file_name3)
                        with open(file_name4, 'a') as outfile4:
                            json.dump(response4, outfile4)
                            print("Done with: " + file_name4)
                    except:
                        print("Could not fetch results for home_airport: " + home_airport + " and destination_airport: " + destination_airport)
                

def stage_in_s3():
    path_to_json = '/usr/local/spark/resources/quotes/'+str(date_today)+'/'
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    for quote in json_files:
        #Read json file
        path = path_to_json + quote
        print("Processing file: "+quote)                    
        key_name = date_today + '/' + quote
        with open(path) as f:
            data1 = json.dumps(json.load(f))
            boto3.client('s3').put_object(Body=data1, Bucket='weekendgetaway-staging', Key=key_name)

        print("Done uploading file to s3: " + key_name)
        print("#########################################")


###############################################
# DAG Definition
###############################################
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2019, 1, 1),
    "email": ["weekendgetaway11@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(dag_id="weekend-getaway-pipeline", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    #Check if the api is working
    is_kiwi_api_available = HttpSensor(
        task_id="is_kiwi_api_available",
        method="GET",
        http_conn_id="kiwi_api",
        endpoint="flights?flyFrom=SFO&to=FCA&dateFrom=18/6/2021&dateTo=28/7/2021&partner=picky&v=3",
        response_check=lambda response: "search_id" in response.text,
        poke_interval=5,
        timeout=20
    )

    download_quotes = PythonOperator(
            task_id="download_quotes",
            python_callable=download_quotes
    )

    stage_in_s3 = PythonOperator(
            task_id="stage_in_s3",
            python_callable=stage_in_s3
    )
    

    is_kiwi_api_available >> download_quotes >> stage_in_s3