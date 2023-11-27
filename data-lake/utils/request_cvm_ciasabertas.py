import utils.source as source

import polars as pl
import pyarrow
import pyarrow.dataset as ds
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
import os


# Check url of the file requested
def url_check(file):
    file = str.lower(file)
    url = ''
    if file == 'ipe':
        url = source.IPE
    elif file == 'fre':
        url = source.FRE
    elif file == 'fca':
        url = source.FCA
    elif file == 'itr':
        url = source.ITR
    elif file == 'dfp':
        url = source.DFP
    elif file == 'cgvn':
        url = source.CGVN
    else:
        print("File not found!")
    return url


def data_lake_path(zone, table):
    if zone == 'raw':
        zone_path = source.RAW+'/'+table
    elif zone == 'trusted':
        zone_path = source.TRUSTED+'/'+table
    elif zone == 'refined':
        zone_path = source.REFINED+'/'+table
    else:
        print("Failed to define the file zone")
    return zone_path


# Send a GET request to the URL
def cvm_files(file):
    file = str.lower(file)
    
    url = url_check(file)
    
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.content, "html.parser")

    # Find all links in the HTML (assuming they represent files)
    links = soup.find_all("a")
    
    # Extract and print the href attribute of each link
    result_list = []
    for link in links:
        href = link.get("href")
        if href and file in href:
            result_list.append(href)
    else:
        print(f"Failed to fetch the page. Status code: {response.status_code}")
    
    return result_list


# Download the most recent file
def download_most_recent(file):
    file = str.lower(file)
    files_list = cvm_files(file)
    url = url_check(file)
    url = url+files_list[-1]
    
    response = requests.get(url)
    download_directory = source.LANDING 

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Get the file name from the URL
        file_name = os.path.join(download_directory, url.split("/")[-1])

        # Save the content of the response to a file
        with open(file_name, "wb") as file:
            file.write(response.content)

        print(f"Downloaded file: {file_name}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")


# Download all files
def download_files(file, year='all'):
    file = str.lower(file)
    files_list = cvm_files(file)
    url = url_check(file)
    
    files = []
    if year == 'all':
        files = files_list
    else:
        for f in files_list:
            if year in f:
                files.append(f)
        
    for f in files:
        f_url = url+f
        response = requests.get(f_url)
        download_directory = source.LANDING 

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Get the file name from the URL
            file_name = os.path.join(download_directory, f_url.split("/")[-1])

            # Save the content of the response to a file
            with open(file_name, "wb") as file:
                file.write(response.content)

            print(f"Downloaded file: {file_name}")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")
        
        
# import most recent file to polars
def read_files_from_landing(file, document='', year='all'):
    file = str.lower(file)
    document = str.upper(document)
    year = str(year)
    files = os.listdir(source.LANDING)

    if year == 'all':
        list_files = [f for f in files if file in f]
    else:
        list_files = [f for f in files if file in f and year in f]
        if list_files == []:
            print(f"Failed to import the file. Check if {file} or {year} are right.")

    list_zip_file_path = [source.LANDING+'/'+f for f in list_files]

    list_polars_df = []
    for path in list_zip_file_path:

        with ZipFile(path, 'r') as zip_file:
            csv_filename = zip_file.namelist()

        if file in ['dfp', 'itr']: 
            file_name = [f for f in csv_filename if document in str.upper(f) and '_con_' in f]
        else:
            file_name = [f for f in csv_filename if document in str.upper(f)]
        
        polars_df = pl.read_csv(
            ZipFile(path).read(file_name[0]),
            truncate_ragged_lines=True,
            encoding='ISO-8859-1',
            separator=';',
            try_parse_dates=True,
            ignore_errors=True
        )
        
        list_polars_df.append(polars_df)
    
    result_df = pl.concat(list_polars_df)
    
    return result_df


def write_files_to_lake(df, zone, table):
    zone = str.lower(zone)
    table = str.lower(table)
    zone_path = data_lake_path(zone, table)
    
    if not os.path.exists(zone_path):
        os.makedirs(zone_path)
            
    df = df.unique()
    
    ds.write_dataset(
        df.to_arrow(),
        zone_path,
        format='parquet'
    )
    
    
def read_files_from_lake(zone, table):
    zone = str.lower(zone)
    table = str.lower(table)
    zone_path = data_lake_path(zone, table)
    
    table = ds.dataset(zone_path, format='parquet').to_table()
    polars_df = pl.DataFrame(table)
    
    return polars_df


def trasnform_trusted_accounts(table):
    
    if 'DT_INI_EXERC' in table.schema:
        table = (
            table
            .with_columns(
                RN_QUARTER = (pl.col('DT_INI_EXERC'))
                .rank('ordinal',descending=True)
                .over(['CNPJ_CIA', 'DT_REFER', 'CD_CONTA'])
                )
            .filter(pl.col('RN_QUARTER')==1)
            )
    
    final_table = (
        table
        .with_columns(
            VER = (pl.col('VERSAO'))
            .rank('ordinal',descending=True)
            .over(['CNPJ_CIA', 'DT_REFER', 'CD_CONTA'])
        )
        .filter(pl.col('VER')==1)
        .with_columns(
            VL_CONTA = pl.when(pl.col('ESCALA_MOEDA') == 'MIL')
            .then(pl.col('VL_CONTA') * 1000)
            .otherwise(pl.col('VL_CONTA'))
        )
        .filter(pl.col('ORDEM_EXERC')=='ÚLTIMO')
        .select(
            pl.col('DT_REFER'),
            pl.col('CNPJ_CIA').alias('CNPJ'),
            pl.col('CD_CVM'),
            pl.col('CD_CONTA'),
            pl.col('DS_CONTA'),
            pl.col('VL_CONTA')
        )
    )
    return final_table


def trasnform_trusted_tickers(table):

    final_table = (
        table
        .filter(
            pl.col('Valor_Mobiliario').str.contains('Ações') |
            pl.col('Valor_Mobiliario').str.contains('Acoes') |
            pl.col('Valor_Mobiliario').str.contains('ações') |
            pl.col('Valor_Mobiliario').str.contains('acoes') |
            (pl.col('Valor_Mobiliario') == 'Unit')
            )
        .filter(pl.col('Mercado')=='Bolsa')
        .with_columns(
            LAST_REPORT = (pl.col('Data_Referencia'))
            .rank('ordinal',descending=True)
            .over(['CNPJ_Companhia', 'Valor_Mobiliario'])
        )
        .filter(pl.col('LAST_REPORT')==1)
        .filter(pl.col('Data_Fim_Negociacao').is_null())
        .filter(pl.col('Data_Fim_Listagem').is_null())
        .filter(pl.col('Codigo_Negociacao').is_not_null())
        .filter(pl.col('Codigo_Negociacao').str.len_chars().is_between(5,6))
        .select(
            pl.col('CNPJ_Companhia').alias('CNPJ'),
            pl.col('Data_Referencia').alias('DT_REFER'),
            pl.col('Segmento').alias('SEGMENTO'),
            pl.col('Codigo_Negociacao').alias('STOCK')
        )
        .unique()
    )
    return final_table


def transform_active_companies():
    
    bpa = read_files_from_lake(zone='trusted', table='itr/bpa')
    
    stocks = read_files_from_lake(zone='trusted', table='fca/valor_mobiliario')
    
    active_companies = (
        bpa
        .select(
            pl.col('CNPJ'),
            pl.col('CD_CVM')
        )
        .unique()
        .join(
            stocks,
            on = ['CNPJ'],
            how = 'inner'
        )
    )
    return active_companies


def trasnform_refined_accounts(document):
    
    active_companies = read_files_from_lake(zone='refined', table='active_companies')
    
    demonstration = read_files_from_lake(zone='raw', table=document)
    
    list_df = [demonstration.filter(pl.col('CNPJ_CIA')==cnpj) for cnpj in tqdm(active_companies['CNPJ'])]
    
    polars_df = pl.concat(list_df)
    
    return polars_df   
    