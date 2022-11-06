import requests
import pandas as pd
from datetime import datetime
import unidecode


"=============================== Getting latitude and longitude of each active weather station in India ====================="
'''
latitude and longitude of weather station taken from World's Air Pollution: Real-time Air Quality Index - https://waqi.info/ 
Refered api doc from World Air Quality Index project - https://aqicn.org/json-api/doc/
For api access first genrate token from - https://aqicn.org/data-platform/token/

bounds : Used only for forward geocoding. This value will restrict the possible results to a defined bounding box.
Values that are not valid coordinates are ignored.
To get bound values used tool - https://opencagedata.com/bounds-finder
values are in form of min lat,min lng,max lat,min lng
'''

token='ddd2233366783b2c533e6082d74b2e2443da1ddc'
boundurl =  "https://api.waqi.info/map/bounds/?latlng=7.36247,67.67578,35.88905,97.38281&token="+token

response = requests.get(boundurl)
data = response.json()['data']
"============================================================================================================================" 

"================================= Storing Latitude , longitude and station data in the dataframe ==========================="
df = pd.json_normalize(data)
for i in df.index:
    if "India" not in df['station.name'][i]:
        df.drop(i, inplace=True)
"============================================================================================================================" 

"============================== Getting the data from API on all the available Locations in INDIA ==========================="
'''
Weather Data collected from OpenWeather API - https://openweathermap.org/
Api doc  for current weather data - https://openweathermap.org/current
note - check free and paid subscriptions calls befor using
For free subscriptions calls are limited per hour,day. May give party not responding error

'''
api_key='38c7b9ba606d2cef6d8ae7b84073a287'
station_data_lst = []
for i in df.index:
    stationUrl ="http://api.openweathermap.org/data/2.5/weather?lat="+str(df['lat'][i])+"&lon="+str(df['lon'][i])+"&units=metric&appid="+api_key
    station_response = requests.get(stationUrl)
    station_data = station_response.json()
    sd_dict = {    
        'cityname': station_data['name'],
        "datetime": datetime.fromtimestamp(station_data['dt']).strftime('%d-%m-%y'),        
        "cloud": station_data['weather'][0]['main'] if 'main' in station_data['weather'][0] else 'None',
        "cloud description": station_data['weather'][0]['description'] if 'description' in station_data['weather'][0] else 'None',       
        "temp": station_data['main']['temp'] if 'temp' in station_data['main'] else 'None',        
        "pressure": station_data['main']['pressure'] if 'pressure' in station_data['main'] else 'None',
        "humidity_percent": station_data['main']['humidity'] if 'humidity' in station_data['main'] else 'None',        
        "visibility_m": station_data['visibility'] if 'visibility' in station_data else 'None',
        "windspeed_mps": station_data['wind']['speed'] if 'speed' in station_data['wind'] else 'None',
        "wind_direction_deg": station_data['wind']['deg']  if 'deg' in station_data['wind'] else 'None',        
        "cloudiness_percent": station_data['clouds']['all'] if 'all' in station_data['clouds'] else 'None',
        "sunrise": datetime.fromtimestamp(station_data['sys']['sunrise']).strftime('%d-%m-%y %H:%M:%S'),
        'sunset': datetime.fromtimestamp(station_data['sys']['sunset']).strftime('%d-%m-%y %H:%M:%S')
    }
    station_data_lst.append(sd_dict)


"============================================= Storing  data into parquet file ==============================================="

data_df=pd.json_normalize(station_data_lst)
data_df['cityname'] = data_df['cityname'].apply(lambda x: unidecode.unidecode(x))
data_df.convert_dtypes()

#print(data_df)
#data_df.to_csv('hdfs://localhost:9000/user/talentum/raw.csv', header=False,index=False)
DataFrame.to_parquet('hdfs://localhost:9000/user/talentum/raw.csv', engine='pyarrow',index=False)

"============================================================================================================================="
