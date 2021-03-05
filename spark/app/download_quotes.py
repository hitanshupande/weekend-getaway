import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from datetime import timedelta
import datetime
import json
import csv
import requests
import boto3
import time
##########################
# You can configure master here if you do not pass the spark.master paramenter in conf
##########################
#master = "spark://spark:7077"
#conf = SparkConf().setAppName("Spark Hello World").setMaster(master)
#sc = SparkContext(conf=conf)
#spark = SparkSession.builder.config(conf=conf).getOrCreate()
# Create spark session

###############################################
# Establish s3 session with credentials
###############################################
session = boto3.Session()
#credentials = session.get_credentials()
#access_key = credentials.access_key
#secret_key = credentials.secret_key
access_key = "AKIAVFOOUIHHNA7LLSUT"
secret_key = "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"
s3 = boto3.resource('s3',
    aws_access_key_id= access_key,
    aws_secret_access_key= secret_key)

###############################################
# API Parameters
###############################################
headers = {
            'x-rapidapi-key': "OhN9UA6HqSEq38AaMSXS5MxsTcWij6xJ",
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
    #d = datetime.datetime.strptime(d, '%d-%m-%Y')
    print("converted d ->", d)
    #d = datetime.datetime.date(int(now.strftime("%Y")), int(now.strftime("%m")), int(now.strftime("%d")))
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

#     next_1_friday = str(next_weekday(d, 4))[0:10]
# #    print(next_1_friday)
#     next_1_sunday = str(next_weekday(datetime.datetime.strptime(next_1_friday, '%d-%m-%Y'), 6))[0:10]

#     next_2_friday = str(next_weekday(datetime.datetime.strptime(next_1_sunday, '%d-%m-%Y'), 4))[0:10]
#     next_2_sunday = str(next_weekday(datetime.datetime.strptime(next_1_sunday, '%d-%m-%Y'), 6))[0:10]

#     next_3_friday = str(next_weekday(datetime.datetime.strptime(next_2_sunday, '%d-%m-%Y'), 4))[0:10]
#     next_3_sunday = str(next_weekday(datetime.datetime.strptime(next_2_sunday, '%d-%m-%Y'), 6))[0:10]

#     next_4_friday = str(next_weekday(datetime.datetime.strptime(next_3_sunday, '%d-%m-%Y'), 4))[0:10]
#     next_4_sunday = str(next_weekday(datetime.datetime.strptime(next_3_sunday, '%d-%m-%Y'), 6))[0:10]

    #Read all airport combinations maintained in airports.csv
    with open('/usr/local/spark/resources/data/airports.csv') as airports:
        reader = csv.DictReader(airports, delimiter=';')
        for row in reader:
            home_airport = row['home_airport']
            destination_airport = row['destination_airport'].split(' ')
            for x in destination_airport:
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
                file_name1 = '/usr/local/spark/resources/quotes/'+home_airport+'_'+x+'_'+'1.json'
                file_name2 = '/usr/local/spark/resources/quotes/'+home_airport+'_'+x+'_'+'2.json'
                file_name3 = '/usr/local/spark/resources/quotes/'+home_airport+'_'+x+'_'+'3.json'
                file_name4 = '/usr/local/spark/resources/quotes/'+home_airport+'_'+x+'_'+'4.json'
                with open(file_name1, 'a') as outfile1:
                    json.dump(response1, outfile1)
                with open(file_name2, 'a') as outfile2:
                    json.dump(response2, outfile2)
                with open(file_name3, 'a') as outfile3:
                    json.dump(response3, outfile3)
                with open(file_name4, 'a') as outfile4:
                    json.dump(response4, outfile4)
            
                
                key_name1 = date_today + '/' + home_airport+'_'+x+'_'+'1.json'
                key_name2 = date_today + '/' + home_airport+'_'+x+'_'+'2.json'
                key_name3 = date_today + '/' + home_airport+'_'+x+'_'+'3.json'
                key_name4 = date_today + '/' + home_airport+'_'+x+'_'+'4.json'
                with open(file_name1) as f1:
                    data1 = json.dumps(json.load(f1))
                    boto3.client('s3').put_object(Body=data1, Bucket='weekendgetaway-staging', Key=key_name1)
                with open(file_name2) as f2:
                    data2 = json.dumps(json.load(f2))
                    boto3.client('s3').put_object(Body=data2, Bucket='weekendgetaway-staging', Key=key_name2)
                with open(file_name1) as f3:
                    data3 = json.dumps(json.load(f3))
                    boto3.client('s3').put_object(Body=data3, Bucket='weekendgetaway-staging', Key=key_name3)
                with open(file_name1) as f4:
                    data4 = json.dumps(json.load(f4))
                    boto3.client('s3').put_object(Body=data4, Bucket='weekendgetaway-staging', Key=key_name4)
                print("Done uploading one combination to s3")
                print("#########################################")
