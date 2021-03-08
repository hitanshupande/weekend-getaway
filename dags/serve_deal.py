from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow import settings
from airflow.models import Connection
import datetime
from datetime import datetime
import os
import time
import json
import datetime as dt
import json
import csv
import requests
import boto3
import time
import psycopg2
import pandas.io.sql as psql

conn_id = Variable.get("conn_id")
conn_type = Variable.get("conn_type")
host = Variable.get("host")
schema = Variable.get("schema")
login = Variable.get("login")
password = Variable.get("password")
port = Variable.get("port")

def extract_deal_payload():
	conn = psycopg2.connect(host=host, dbname=schema, user=login, password=password, port=port)
	df = psql.read_sql('SELECT * FROM subscribed_users', conn)
	
	for index, row in df.iterrows():
		email = row['email']
		home_airport = row['home_airport_id']

		df_deals_for_user = psql.read_sql("""
	    	select a.origin_airport_cd, a.destination_airport_cd, a.quote_price, a.average_price, a.deal_date, a.deal_value, a.min_price, a.max_price, b.deep_link
			from (SELECT * FROM deals where origin_airport_cd =  %(airport)s order by deal_value DESC limit 3) a LEFT JOIN quote_fact b
			ON a.origin_airport_cd = b.origin_airport_cd
			AND a.destination_airport_cd = b.destination_airport_cd
			AND a.deal_date = b.date_of_api_pull_key
			"""
	    	, conn, params={"airport":home_airport})
		#Generate json file from query output
		name_of_file = "deal_"+str(index)+".json"
		df_deals_for_user.to_json(name_of_file)

		#Move file to a deals folder	
		path_to_move = 'deals/'+name_of_file
		os.rename(name_of_file, path_to_move)

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

#Return todays date as String
def todays_date():
    now = datetime.now()
    temp = now.strftime("%d")+'-'+now.strftime("%m")+'-'+now.strftime("%Y")
    return temp

def stage_in_s3():
    path_to_json = 'deals/'
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    for quote in json_files:
        #Read json file
        path = path_to_json + quote
        print("Processing file: "+quote)                    
        key_name = 'deals/'+ todays_date() + '/' + quote
        with open(path) as f:
            data1 = json.dumps(json.load(f))
            boto3.client('s3').put_object(Body=data1, Bucket='weekendgetaway-staging', Key=key_name)

        print("Done uploading file to s3: " + key_name)
        print("#########################################")



dag_params = {
    'dag_id': 'serve_deal_dag',
    'start_date': datetime(2019, 10, 7),
    'schedule_interval': None
}


with DAG(**dag_params) as dag:
	extract_deal_payload = PythonOperator(
		task_id='extract_deal_payload',
		python_callable=extract_deal_payload
	)
	
	sending_email = EmailOperator(
		task_id="sending_email",
		to="hitanshu.pande@gmail.com",
		subject="Deals from Weekend Getaway",
		html_content="<h3> Deals from Weekend Getaway </h3>"
	)

	stage_in_s3 = PythonOperator(
            task_id="stage_in_s3",
            python_callable=stage_in_s3
    )
	
	extract_deal_payload >> sending_email
	extract_deal_payload.set_downstream(stage_in_s3)
