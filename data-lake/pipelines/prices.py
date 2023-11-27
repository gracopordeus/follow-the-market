import time
from tqdm import tqdm

from utils.request_market_data import (
    request_prices_to_landing,
    write_prices_to_raw,
    trasnform_trusted_prices
)

from utils.request_cvm_ciasabertas import (
    read_files_from_lake,
    write_files_to_lake
)



df = read_files_from_lake(
    zone='refined', 
    table='active_companies'
    )


for stock in tqdm(df['STOCK']):
    try:
        request_prices_to_landing(ticker=stock)
    except Exception as e:
        print(f"An error occurred: {e}")
        continue
    
    time.sleep(14)
        
    write_prices_to_raw(ticker=stock)


prices = trasnform_trusted_prices(ticker_list=df['STOCK'])

write_files_to_lake(
    df = prices,
    zone = 'trusted',
    table = 'prices'
    )