import numpy as np
import pandas as pd

import time
import requests
from io import StringIO
from datetime import datetime

class DataExtractor():
    def __init__(self, tickers, apikey):
        self.database = {}
        self.tickers = tickers
        self.apikey = apikey

    # Get Data Functions

    @staticmethod
    def get_daily_data(apikey, ticker):
        print("Retrieving daily data")
        
        time_length = 'TIME_SERIES_DAILY'
        datatype = 'csv'
        
        url = f'https://www.alphavantage.co/query?function={time_length}&symbol={ticker}&outputsize=full&apikey={apikey}&datatype={datatype}'
        
        response = requests.get(url)

        data_string = StringIO(response.text)
        data = pd.read_csv(data_string)[::-1]
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data.rename(columns={'timestamp': 'date'}, inplace=True)
        data.set_index('date', drop=True, inplace=True)
        
        return data
    
    @staticmethod
    def get_hourly_data(apikey, ticker, start_year, end_year):
        print("Retrieving hourly data")

        time_length = 'TIME_SERIES_INTRADAY'
        interval_length = '60min'
        datatype = 'csv'
        
        hourly_data = pd.DataFrame()
        
        for year in range(start_year, end_year+1):
            for month in range(1,13):
                if month < 10:
                    timestamp = f'{year}-0{month}'
                else:
                    timestamp = f'{year}-{month}'
                
                url = f'https://www.alphavantage.co/query?function={time_length}&symbol={ticker}&interval={interval_length}&outputsize=full&apikey={apikey}&datatype={datatype}&month={timestamp}'
                response = requests.get(url)

                data_string = StringIO(response.text)
                df = pd.read_csv(data_string)[::-1]
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.rename(columns={'timestamp': 'date'}, inplace=True)
                df.set_index('date', drop=True, inplace=True)
                hourly_data = pd.concat([hourly_data, df], axis=0)
                time.sleep(13)
        
        return hourly_data
    
    @staticmethod
    def get_5min_data(apikey, ticker, start_year, end_year):
        print("Retrieving 5 minute data")

        time_length = 'TIME_SERIES_INTRADAY'
        interval_length = '5min'
        datatype = 'csv'
        
        min_data = pd.DataFrame()
        
        for year in range(start_year, end_year+1):
            for month in range(1,13):
                if month < 10:
                    timestamp = f'{year}-0{month}'
                else:
                    timestamp = f'{year}-{month}'
                
                url = f'https://www.alphavantage.co/query?function={time_length}&symbol={ticker}&interval={interval_length}&outputsize=full&apikey=LSY1P7SPJOP4UKMB&datatype={datatype}&month={timestamp}'
                response = requests.get(url)

                data_string = StringIO(response.text)
                df = pd.read_csv(data_string)[::-1]
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.rename(columns={'timestamp': 'date'}, inplace=True)
                df.set_index('date', drop=True, inplace=True)
                min_data = pd.concat([min_data, df], axis=0)
                time.sleep(13)
        
        return min_data

    # Technical Indicator Functions

    @staticmethod
    def camarilla_indicator(data):
        print("Adding camarilla indicator")

        data_shifted = data.shift(1)  # Shifting the data one step upward

        data['R4'] = data_shifted['close'] + (data_shifted['high'] - data_shifted['low']) * 1.1/2
        data['R3'] = data_shifted['close'] + (data_shifted['high'] - data_shifted['low']) * 1.1/4
        data['S3'] = data_shifted['close'] - (data_shifted['high'] - data_shifted['low']) * 1.1/4
        data['S4'] = data_shifted['close'] - (data_shifted['high'] - data_shifted['low']) * 1.1/2

    @staticmethod
    def ma_indicator(data, window):
        print(f"Adding moving average of window {window} indicator")

        data_shifted = data.shift(1)
        
        data[f'MA {window}'] = data_shifted['close'].rolling(window).mean()

    @staticmethod
    def camarilla_check(data):
        print("Adding camarilla narrow checker indicator")

        today_camarilla = data.shift(1)[['R4', 'R3', 'S3', 'S4']]
        today_camarilla.columns = ['PR4', 'PR3', 'PS3', 'PS4']
        concat = pd.concat([data, today_camarilla], axis=1)

        data['narrow'] = 0
        data.loc[(concat['PR4'] > concat['R4']) & (concat['PR3'] > concat['R3']) & (concat['PS4'] < concat['S4']) & (concat['PS3'] < concat['S3']), 'narrow'] = 1

    # Get Time Functions

    @staticmethod
    def convert_to_datetime(date):
        return pd.to_datetime(date)

    @staticmethod
    def get_date(date_and_time):
        return str(date_and_time).split(' ')[0]

    @staticmethod
    def get_hour(date_and_time):
        timestamp = str(date_and_time).split(' ')[1]
        date = DataExtractor.get_date(date_and_time)
        complete_hour = date + ' ' + timestamp.split(':')[0] + ':00:00'
        return complete_hour

    # Database functions

    def add_hourly_to_5min(self):
        print("Merging hourly to five minute data")

        for ticker in self.tickers:
            five_min_data = self.database[ticker]['5min']
            hourly_data = self.database[ticker]['hourly']
            for index in five_min_data.index:
                hour = self.get_hour(index)
                
                if hour in hourly_data.index:
                    five_min_data.loc[index, 'hourly close'] = hourly_data.loc[hour, 'close']
                    five_min_data.loc[index, 'R4'] = hourly_data.loc[hour, 'R4']
                    five_min_data.loc[index, 'R3'] = hourly_data.loc[hour, 'R3']
                    five_min_data.loc[index, 'S3'] = hourly_data.loc[hour, 'S3']
                    five_min_data.loc[index, 'S4'] = hourly_data.loc[hour, 'S4']
                    five_min_data.loc[index, 'narrow'] = hourly_data.loc[hour, 'narrow']
                    five_min_data.loc[index, 'MA 200'] = hourly_data.loc[hour, 'MA 200']
                    five_min_data.loc[index, 'MA 50'] = hourly_data.loc[hour, 'MA 50']

    def add_daily_to_hourly(self):
        print("Merging daily to hourly data")

        for ticker in self.tickers:
            daily_data = self.database[ticker]['daily']
            hourly_data = self.database[ticker]['hourly']
            for index in hourly_data.index:
                date = self.get_date(index)
                
                if date in daily_data.index:
                    hourly_data.loc[index, 'daily close'] = daily_data.loc[date, 'close']
                    hourly_data.loc[index, 'R4'] = daily_data.loc[date, 'R4']
                    hourly_data.loc[index, 'R3'] = daily_data.loc[date, 'R3']
                    hourly_data.loc[index, 'S3'] = daily_data.loc[date, 'S3']
                    hourly_data.loc[index, 'S4'] = daily_data.loc[date, 'S4']
                    hourly_data.loc[index, 'narrow'] = daily_data.loc[date, 'narrow']
                
    def drop_all_na(self):
        print("Dropping all null values")

        for ticker in self.tickers:
            daily_data = self.database[ticker]['daily']
            hourly_data = self.database[ticker]['hourly']
            daily_data.dropna(inplace=True)
            hourly_data.dropna(inplace=True)
            
    def get_data(self, start_year, end_year):
        print(f"Getting data from {start_year} to {end_year} inclusively")

        for ticker in self.tickers:
            daily_data = self.get_daily_data(self.apikey, ticker)
            hourly_data = self.get_hourly_data(self.apikey, ticker, start_year, end_year)
            five_min_data = self.get_5min_data(self.apikey, ticker, start_year, end_year)
            self.database[ticker] = {'daily': daily_data, 'hourly': hourly_data, '5min': five_min_data}
            
    def add_indicators(self):
        print("Adding indicators")
        for ticker in self.tickers:
            daily_data = self.database[ticker]['daily']
            hourly_data = self.database[ticker]['hourly']
            
            self.camarilla_indicator(daily_data)
            self.camarilla_check(daily_data)
            self.ma_indicator(hourly_data, 50)
            self.ma_indicator(hourly_data, 200)
            
    def save_data_csv(self, period):
        print("saving data to csv")
        for ticker in self.tickers:
            self.database[ticker]['daily'].to_csv(f'./stock_data/daily_data/{ticker}_{period}')
            self.database[ticker]['hourly'].to_csv(f'./stock_data/hourly_data/{ticker}_{period}')
            self.database[ticker]['5min'].to_csv(f'./stock_data/five_minute_data/{ticker}_{period}')
        

with open('apikey.txt', 'r') as file:
    apikey = file.read()

tickers = ["SPY"]
stock_database = DataExtractor(tickers, apikey)
stock_database.get_data(2016, 2018)
stock_database.add_indicators()
stock_database.add_daily_to_hourly()
stock_database.add_hourly_to_5min()
stock_database.drop_all_na()
stock_database.save_data_csv("2016-2018")