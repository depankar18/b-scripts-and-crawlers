"""
EMS CRAWLER (Device 7)

Made By - Depankar Bisoy
Roll No. - 18CS8164
Department - Computer Science and Engineering

NIT Durgapur, West Bengal

"""


from pymongo import MongoClient
from datetime import datetime, date
import requests, time, calendar, pandas as pd

with open("mongo_pass.txt","r") as f:
    password=f.read()
with open("mongo_user.txt","r") as f:
    username=f.read()

while(True) :
    
    try :
        print("EMS (Device 7) Crawler is up")

        # CONNECTING TO MONGODB DATABASE
        cluster=MongoClient("mongodb+srv://"+username+":"+password+"@cluster3.zirzh.mongodb.net/database_3?retryWrites=false&w=majority")
        db=cluster['database_3']
        ems_d=db['ems_crawler_dv7']

        # GETTING THE CURRENT DATE FOR API CALL
        date_today=str(date.today())
        pattern='%Y-%m-%d'

        # PRINTING THE CONNECTION SUCCESSFUL MESSAGE WITH PROPER DATE AND TIME
        utc_time=datetime.utcnow()
        time_now=utc_time.strftime("%H:%M:%S")
        print('Connection Successful for '+date_today+' '+str(time_now)+' [UTC]')

        # FETCHING THE EMS DATA (DEVICE 7) AND SAVING IT
        response=requests.get("http://iotbuilder.in/nit-dp/view.php?id=7")
        with open("response.xls", "w") as f:
            f.write(response.text)

        # READING THE DATA FROM THE SAVED FILE
        data=pd.read_html("response.xls")[0]

        for i in range(2999,-1,-1):
            if(ems_d.find_one({'date' : str(data['Date'][i])})==None):
                temp=int(data['Temperature'][i])
                humidity=int(data['Humidity'][i])
                co=int(data['Carbon Monoxide'][i])
                no2=float(data['Nitrogen Dioxide'][i])
                pm2_5=int(data['Dust (PM2.5)'][i])
                pm10=int(data['Dust (PM10)'][i])
                date=str(data['Date'][i])

                ems_d.insert_one({'temp':temp, 'humidity':humidity, 'co':co, 'no2':no2, 'pm2_5':pm2_5, 'pm10':pm10, 'date':date})

        #requests.get("http://mail:7500/notify?message=Connected successfully to MongoDB "+date_today+" "+str(time_now)+" [UTC]. Database has been updated with EMS (Device 7) data.")
        print("Mail sent successfully")

        print("SLEEPING")
        time.sleep(86400) # SLEEP TIME IS 24 HRS

    except:
        print("Connection Error!!!")

        #requests.get("http://mail:7500/notify?message=Connection Error!!!")
        time.sleep(600) # SLEEP FOR 10 MINUTES AND THEN RETRY

