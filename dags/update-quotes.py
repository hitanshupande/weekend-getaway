from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow import settings
from airflow.models import Connection
from airflow.models import Variable
import datetime
from datetime import datetime
import os
import time
import json
import datetime as dt


conn_id = Variable.get("conn_id")
conn_type = Variable.get("conn_type")
host = Variable.get("host")
schema = Variable.get("schema")
login = Variable.get("login")
password = Variable.get("password")
port = Variable.get("port")
now = datetime.now()
date_of_api_pull = now.strftime("%Y")+'-'+now.strftime("%m")+'-'+now.strftime("%d")

conn = Connection(conn_id=conn_id,conn_type=conn_type,host=host,login=login,password=password,schema=schema,port=port)
session = settings.Session() # get the session
session.add(conn)
session.commit()


def todays_date():
    now = datetime.now()
    temp = now.strftime("%d")+'-'+now.strftime("%m")+'-'+now.strftime("%Y")
    return temp


def generate_inserts_for_quote_fact():
	path_to_json = '/usr/local/spark/resources/quotes/'+todays_date()+'/'
	json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
	
	fileName = todays_date() + ".sql"
	file = open(fileName, "w")

	for quote in json_files:
		path = path_to_json + quote
		print("Processing file"+quote)
		with open(path) as f:
			try:
				data = json.load(f)
				flyFrom = (data["data"][0]["flyFrom"])
				flyTo = (data["data"][0]["flyTo"])
				date_departure_key_epoch = (data["data"][0]["dTimeUTC"])
				date_departure_key = datetime.utcfromtimestamp(date_departure_key_epoch).strftime("%Y%m%d")
				date_return_key_epoch = (data["data"][0]["aTimeUTC"])
				date_return_key = datetime.utcfromtimestamp(date_departure_key_epoch).strftime("%Y%m%d")
				quote_price = (data["data"][0]["price"])
				direct_flight_children = len(data["data"][0]["route"])
				if direct_flight_children > 1 :
					direct_flight = False
				else: 
					direct_flight = True
				quote_datetime = datetime.now()
				now = datetime.now()
				date_of_api_pull_key = now.strftime("%Y")+'-'+now.strftime("%m")+'-'+now.strftime("%d")
				deep_link = (data["data"][0]["deep_link"])
				insert_statement = "Insert into quote_fact values (default," + "'"+flyFrom+"', '"+ flyTo + "', " + str(date_departure_key) + ", " + str(date_return_key) + ", " + str(quote_price) + ", " + str(direct_flight) + ", " + "'" + str(quote_datetime) + "', '" +str(date_of_api_pull_key) + "', '"+ deep_link + "');" + "\n"
				file.write(insert_statement)
			except:
				print("Couldn't process "+quote) 
			

	file.close()
	path_to_move = 'sql/'+fileName
	print("fileName: "+fileName)
	print("newPath: "+path_to_move)
	os.rename(fileName, path_to_move)

dag_params = {
    'dag_id': 'PostgresOperator_dag',
    'start_date': datetime(2019, 10, 7),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

	generate_inserts_for_quote_fact = PythonOperator(
		task_id='generate_inserts_for_quote_fact',
		python_callable=generate_inserts_for_quote_fact
	)

	update_table = PostgresOperator(
	task_id='update_table',
	postgres_conn_id='postgres_connection',
	sql='sql/'+todays_date()+'.sql'
	)

	generate_deal = PostgresOperator(
	task_id='generate_deal',
	postgres_conn_id='postgres_connection',
	sql='sql/gen_deal.sql',
	params={'date_of_api_pull': date_of_api_pull}
	)

	generate_inserts_for_quote_fact >> update_table >> generate_deal