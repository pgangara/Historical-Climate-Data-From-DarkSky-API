import datetime
import pandas as pd
import forecastio
import getpass
import pyodbc
import os
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=Server Name;DATABASE=master;trusted_connection=yes')
query = """ Query to get zipcodes """

df = pd.read_sql(query,cnxn)
df = df.to_dict('records')

api_key = 'your-key'

date = datetime.datetime(2015,1,1)
latitude = 40.5956
longitude = -82.1129


data = []
for i in range(0,len(df)):
    latitude = df[i]['Latitude']
    longitude = df[i]['Longitude']

    start = datetime.datetime(2017, 1, 29)
    for offset in range(1, 365):
        print(offset)
        forecast = forecastio.load_forecast(api_key, latitude, longitude, time=start+datetime.timedelta(offset), units="us")
        byHour = forecast.daily()
        d = byHour.data
        temperatureHigh = d[0].temperatureHigh
        temperatureLow = d[0].temperatureLow
        datee = d[0].time
        summary = d[0].summary
        icon = d[0].icon
        data.append({'Zipcode':df[i]['ZLZIP'],
                    'HistoryDate':datee,
                    'Summary':summary,
                    'Icon':icon,
                    'TemperatureHigh': temperatureHigh,
                    'TemperatureLow':temperatureLow})




dff = pd.DataFrame(data)

def df_sql(df,cnxn):
    cursor = cnxn.cursor()

    for index,row in df.iterrows():
        cursor.execute("INSERT INTO Table_Name([HistoryDate],[Zipcode],[Summary],[Icon],[TemperatureHigh],[TemperatureLow]) values (?,?,?,?,?,?)",row['HistoryDate'],row['Zipcode'],row['Summary'],row['Icon'],row['TemperatureHigh'],row['TemperatureLow'])
        cnxn.commit()
    cursor.close()
    cnxn.close()


dff = dff[['HistoryDate','Zipcode','Summary','Icon','TemperatureHigh','TemperatureLow']]
df_sql(dff,cnxn)
