import pandas as pd
import requests
import os
from dotenv import load_dotenv
load_dotenv()


city_name = "Yangon"
api_key = os.getenv("WEATHER_KEY")

weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"

weather_data = requests.get(weather_url).json()

weather_data_df = pd.json_normalize(weather_data)

print(weather_data_df.head())