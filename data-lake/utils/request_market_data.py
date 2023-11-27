
import pyarrow.dataset as ds
import polars as pl
import requests
import json
import os
from tqdm import tqdm

import utils.source as source


def request_prices_to_landing(ticker):
    symbol = str.upper(ticker)+'.SA'
    
    url = "https://alpha-vantage.p.rapidapi.com/query"

    querystring = {
        "function":"TIME_SERIES_DAILY_ADJUSTED",
        "symbol":symbol,
        "outputsize":"full",
        "datatype":"json"
    }

    headers = {
        "X-RapidAPI-Key": "4f7a60bea9msh71caebc682f804fp132d5cjsn03a10bff5df3",
        "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    data = response.json()

    # Specify the JSON file path
    json_file_path = source.LANDING+'/prices/stock_price_'+str.lower(ticker)+'.json'

    # Write the data to the JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f"Downloaded file: {json_file_path}")
   

def write_prices_to_raw(ticker):
    file = str.lower('ticker')
    zone_path = source.RAW+'/prices/'+ticker
    json_file_path = source.LANDING+'/prices/stock_price_'+str.lower(ticker)+'.json'

    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Extract relevant information
    time_series_data = data.get("Time Series (Daily)", {})
    formatted_data = []
    
    for date, values in time_series_data.items():
        row = {
            "date": date,
            "open": float(values.get("1. open")),
            "high": float(values.get("2. high")),
            "low": float(values.get("3. low")),
            "close": float(values.get("4. close")),
            "adjusted_close": float(values.get("5. adjusted close")),
            "volume": int(values.get("6. volume")),
            "dividend_amount": float(values.get("7. dividend amount")),
            "split_coefficient": float(values.get("8. split coefficient")),
        }
        formatted_data.append(row)

    # Create a DataFrame with the specified data types
    df = pl.DataFrame({
        "date": [row["date"] for row in formatted_data],
        "open": [row["open"] for row in formatted_data],
        "high": [row["high"] for row in formatted_data],
        "low": [row["low"] for row in formatted_data],
        "close": [row["close"] for row in formatted_data],
        "adjusted_close": [row["adjusted_close"] for row in formatted_data],
        "volume": [row["volume"] for row in formatted_data],
        "dividend_amount": [row["dividend_amount"] for row in formatted_data],
        "split_coefficient": [row["split_coefficient"] for row in formatted_data],
    })
    
    df = df.with_columns(pl.col('date').cast(pl.Date))
    
    if not os.path.exists(zone_path):
        os.makedirs(zone_path)

    # Save the DataFrame to a CSV file
    ds.write_dataset(
        df.to_arrow(),
        zone_path,
        format='parquet',
        existing_data_behavior='overwrite_or_ignore'
    )
    
    print(f"Data saved to: {zone_path}")
    
    
def read_prices_from_lake(zone, ticker):
    zone = str.lower(zone)
    ticker = str.upper(ticker)
    zone_path = source.RAW+'/prices/'+ticker
    
    table = ds.dataset(zone_path, format='parquet').to_table()
    polars_df = pl.DataFrame(table)
    
    return polars_df

def trasnform_trusted_prices(ticker_list):
    
    list_df = []
    
    for ticker in tqdm(ticker_list):
        ticker = str.upper(ticker)
        
        try:
            prices = (
            read_prices_from_lake('raw', ticker)
            .with_columns(STOCK = pl.lit(ticker))
            .filter(pl.col('date').dt.year() >= 2010)
            )
            
            list_df.append(prices)
        except Exception as e:
            continue
        
    polars_df = pl.concat(list_df)
    
    return polars_df