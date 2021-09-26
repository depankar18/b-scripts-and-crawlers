"""
WEATHER CRAWLER FOR OPEN WEATHER MAP API

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
with open("owm_user_api.txt","r") as f:
    user_api=f.read()

while(True) :
    
    try :
        print("Weather Crawler is up")
        # CONNECTING TO MONGODB DATABASE
        cluster=MongoClient("mongodb+srv://"+username+":"+password+"@cluster1.dcz0z.mongodb.net/database_2?retryWrites=false&w=majority")
        db=cluster['database_2']
        w_d=db['weather_crawler']

        # GETTING THE CURRENT DATE FOR API CALL
        date_today=str(date.today())
        pattern='%Y-%m-%d'

        # PRINTING THE CONNECTION SUCCESSFUL MESSAGE WITH PROPER DATE AND TIME
        utc_time=datetime.utcnow()
        time_now=utc_time.strftime("%H:%M:%S")
        print('Connection Successful for '+date_today+' '+str(time_now)+' [UTC]')


        # PREVIOUS 3RD DAY FROM TODAY
        epoch_dt_3=str(int(calendar.timegm(time.strptime(date_today, pattern)))-(86400*3))
        full_api_link_3="https://api.openweathermap.org/data/2.5/onecall/timemachine?lat=23.520445&lon=87.311920&dt="+epoch_dt_3+"&appid="+user_api
        api_link_3=requests.get(full_api_link_3)
        data_3=api_link_3.json() # JSON DATA FOR PREVIOUS 3RD DAY FROM TODAY

        # PREVIOUS 2ND DAY FROM TODAY
        epoch_dt_2=str(int(calendar.timegm(time.strptime(date_today, pattern)))-(86400*2))
        full_api_link_2="https://api.openweathermap.org/data/2.5/onecall/timemachine?lat=23.520445&lon=87.311920&dt="+epoch_dt_2+"&appid="+user_api
        api_link_2=requests.get(full_api_link_2)
        data_2=api_link_2.json() # JSON DATA FOR PREVIOUS 2ND DAY FROM TODAY

        # PREVIOUS DAY FROM TODAY
        epoch_dt_1=str(int(calendar.timegm(time.strptime(date_today, pattern)))-86400)
        full_api_link_1="https://api.openweathermap.org/data/2.5/onecall/timemachine?lat=23.520445&lon=87.311920&dt="+epoch_dt_1+"&appid="+user_api
        api_link_1=requests.get(full_api_link_1)
        data_1=api_link_1.json() # JSON DATA FOR PREVIOUS DAY FROM TODAY


        # PREVIOUS 3RD DAY COMPARE AND INSERT
        for i in range(24):
            if(w_d.find_one({'date_time_utc' : datetime.utcfromtimestamp(data_3['hourly'][i]['dt'])})==None):

                date_time_utc=datetime.utcfromtimestamp(data_3['hourly'][i]['dt'])
                temp=data_3['hourly'][i]['temp']
                temp_feel=data_3['hourly'][i]['feels_like']
                pressure=data_3['hourly'][i]['pressure']
                humidity=data_3['hourly'][i]['humidity']
                dew_point=data_3['hourly'][i]['dew_point']
                uvi=data_3['hourly'][i]['uvi']
                clouds=data_3['hourly'][i]['clouds']
                visibility=data_3['hourly'][i]['visibility']
                wind_speed=data_3['hourly'][i]['wind_speed']
                wind_deg=data_3['hourly'][i]['wind_deg']
                weather_id=data_3['hourly'][i]['weather'][0]['id']
                weather_main=data_3['hourly'][i]['weather'][0]['main']
                weather_desc=data_3['hourly'][i]['weather'][0]['description']
                weather_icon=data_3['hourly'][i]['weather'][0]['icon']

                w_d.insert_one({'date_time_utc':date_time_utc, 'temp':temp, 'temp_feel':temp_feel, 'pressure':pressure, 'humidity':humidity, 'dew_point':dew_point, 'uvi':uvi, 'clouds':clouds,  'visibility':visibility, 'wind_speed':wind_speed, 'wind_deg':wind_deg, 'weather_id':weather_id, 'weather_main':weather_main, 'weather_desc':weather_desc, 'weather_icon':weather_icon})

        # PREVIOUS 2ND DAY COMPARE AND INSERT
        for i in range(24):
            if(w_d.find_one({'date_time_utc' : datetime.utcfromtimestamp(data_2['hourly'][i]['dt'])})==None):

                date_time_utc=datetime.utcfromtimestamp(data_2['hourly'][i]['dt'])
                temp=data_2['hourly'][i]['temp']
                temp_feel=data_2['hourly'][i]['feels_like']
                pressure=data_2['hourly'][i]['pressure']
                humidity=data_2['hourly'][i]['humidity']
                dew_point=data_2['hourly'][i]['dew_point']
                uvi=data_2['hourly'][i]['uvi']
                clouds=data_2['hourly'][i]['clouds']
                visibility=data_2['hourly'][i]['visibility']
                wind_speed=data_2['hourly'][i]['wind_speed']
                wind_deg=data_2['hourly'][i]['wind_deg']
                weather_id=data_2['hourly'][i]['weather'][0]['id']
                weather_main=data_2['hourly'][i]['weather'][0]['main']
                weather_desc=data_2['hourly'][i]['weather'][0]['description']
                weather_icon=data_2['hourly'][i]['weather'][0]['icon']

                w_d.insert_one({'date_time_utc':date_time_utc, 'temp':temp, 'temp_feel':temp_feel, 'pressure':pressure, 'humidity':humidity, 'dew_point':dew_point, 'uvi':uvi, 'clouds':clouds,  'visibility':visibility, 'wind_speed':wind_speed, 'wind_deg':wind_deg, 'weather_id':weather_id, 'weather_main':weather_main, 'weather_desc':weather_desc, 'weather_icon':weather_icon})

        # PREVIOUS DAY COMPARE AND INSERT
        for i in range(24):
            if(w_d.find_one({'date_time_utc' : datetime.utcfromtimestamp(data_1['hourly'][i]['dt'])})==None):

                date_time_utc=datetime.utcfromtimestamp(data_1['hourly'][i]['dt'])
                temp=data_1['hourly'][i]['temp']
                temp_feel=data_1['hourly'][i]['feels_like']
                pressure=data_1['hourly'][i]['pressure']
                humidity=data_1['hourly'][i]['humidity']
                dew_point=data_1['hourly'][i]['dew_point']
                uvi=data_1['hourly'][i]['uvi']
                clouds=data_1['hourly'][i]['clouds']
                visibility=data_1['hourly'][i]['visibility']
                wind_speed=data_1['hourly'][i]['wind_speed']
                wind_deg=data_1['hourly'][i]['wind_deg']
                weather_id=data_1['hourly'][i]['weather'][0]['id']
                weather_main=data_1['hourly'][i]['weather'][0]['main']
                weather_desc=data_1['hourly'][i]['weather'][0]['description']
                weather_icon=data_1['hourly'][i]['weather'][0]['icon']
    
                w_d.insert_one({'date_time_utc':date_time_utc, 'temp':temp, 'temp_feel':temp_feel, 'pressure':pressure, 'humidity':humidity, 'dew_point':dew_point, 'uvi':uvi, 'clouds':clouds,  'visibility':visibility, 'wind_speed':wind_speed, 'wind_deg':wind_deg, 'weather_id':weather_id, 'weather_main':weather_main, 'weather_desc':weather_desc, 'weather_icon':weather_icon})
        
        #requests.get("http://mail:7500/notify?message=Connected successfully to MongoDB "+date_today+" "+str(time_now)+" [UTC]. Database has been updated with latest weather data.")
        print("Mail sent successfully")

        print("SLEEPING")
        time.sleep(86400) # SLEEP TIME IS 24 HRS
        # NEW UNIQUE DATA WILL BE INSERTED IN THE DATABASE AFTER EVERY 24 HRS

    except:
        print("Connection Error!!!")

        #requests.get("http://mail:7500/notify?message=Connection Error!!!")
        time.sleep(600) # SLEEP FOR 10 MINUTES AND THEN RETRY

