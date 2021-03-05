from datetime import timedelta
import datetime
import json
import csv
import requests
import boto3
import time


#Read json file
path = '/usr/local/spark/app/resources/quotes/BOS_ANC_1.json'
with open(path) as f:
	data = json.load(f)
	print(data)