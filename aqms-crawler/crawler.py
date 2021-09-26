"""
AQMS CRAWLER

Made By - Depankar Bisoy
Roll No. - 18CS8164
Department - Computer Science and Engineering

NIT Durgapur, West Bengal

"""


from pymongo import MongoClient
from datetime import datetime, date
import requests, time, calendar

with open("mongo_pass.txt","r") as f:
    password=f.read()
with open("mongo_user.txt","r") as f:
    username=f.read()
with open("aqicn_api.txt","r") as f:
    user_api=f.read()

while(True) :
    
    try :
        print("AQMS Crawler is up")
        # CONNECTING TO MONGODB DATABASE
        cluster=MongoClient("mongodb+srv://"+username+":"+password+"@cluster2.wnjrs.mongodb.net/database_1?retryWrites=false&w=majority")
        db=cluster['database_1']
        aqms_d=db['aqms_crawler']

        # GETTING THE CURRENT DATE FOR API CALL
        date_today=str(date.today())
        pattern='%Y-%m-%d'

        # PRINTING THE CONNECTION SUCCESSFUL MESSAGE WITH PROPER DATE AND TIME
        utc_time=datetime.utcnow()
        time_now=utc_time.strftime("%H:%M:%S")
        print('Connection Successful for '+date_today+' '+str(time_now)+' [UTC]')

        # DATA COLLECTION
        full_api_link="https://api.waqi.info/feed/india/durgapur/sidhu-kanhu-indoor-stadium/?token="+user_api
        api_link=requests.get(full_api_link)
        data=api_link.json()

        # DATA COMPARE AND INSERT
        if(aqms_d.find_one({'date_time_ist' : str(data['data']['time']['s'])})==None):
            date_time_ist=data['data']['time']['s']
            aqi=data['data']['aqi']
            temp=data['data']['iaqi']['t']['v']
            humidity=data['data']['iaqi']['h']['v']
            pressure=data['data']['iaqi']['p']['v']
            wind=data['data']['iaqi']['w']['v']
            pm2_5=data['data']['iaqi']['pm25']['v']
            pm10=data['data']['iaqi']['pm10']['v']
            no2=data['data']['iaqi']['no2']['v']
            so2=data['data']['iaqi']['so2']['v']

            aqms_d.insert_one({'date_time_ist':date_time_ist, 'aqi':aqi, 'temp':temp, 'humidity':humidity, 'pressure':pressure, 'wind':wind, 'pm2_5':pm2_5, 'pm10':pm10, 'no2':no2, 'so2':so2})

        #requests.get("http://mail:7500/notify?message=Connected successfully to MongoDB "+date_today+" "+str(time_now)+" [UTC]. Database has been updated with latest AQMS data.")
        print("Mail sent successfully")

        print("SLEEPING")
        time.sleep(1800) # SLEEP TIME IS 30 MINUTES

    except:
        print("Connection Error!!!")

        #requests.get("http://mail:7500/notify?message=Connection Error!!!")
        time.sleep(600) # SLEEP FOR 10 MINUTES AND THEN RETRY

