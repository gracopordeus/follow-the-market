import source

import polars as pl
import pyarrow
import pyarrow.dataset as ds
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
import os


# Check url of the file requested
def cvmUrlCheck(file):
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


def dataLakePath(zone, table):
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
def cvmFiles(file):
    file = str.lower(file)
    
    url = cvmUrlCheck(file)
    
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
def cvmDownloadMostRecent(file):
    file = str.lower(file)
    files_list = cvmFiles(file)
    url = cvmUrlCheck(file)
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
def cvmDownloadFiles(file, year='all'):
    file = str.lower(file)
    files_list = cvmFiles(file)
    url = cvmUrlCheck(file)
    
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
def cvmReadFromLanding(file, document, year='all'):
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
            file_name = [f for f in csv_filename if document in f and '_con_' in f]
        else:
            file_name = [f for f in csv_filename]
        
        polars_df = pl.read_csv(
            ZipFile(path).read(file_name[0]),
            truncate_ragged_lines=True,
            encoding='ISO-8859-1',
            separator=';',
            try_parse_dates=True
        )
        
        list_polars_df.append(polars_df)
    
    result_df = pl.concat(list_polars_df)
    
    return result_df


def cvmWriteFiles(df, zone, table):
    zone = str.lower(zone)
    table = str.lower(table)
    zone_path = dataLakePath(zone, table)
    
    if not os.path.exists(zone_path):
        os.makedirs(zone_path)
            
    if zone == 'raw':
        df = df.with_columns([
            (pl.col('DT_REFER').dt.year()).alias('YEAR'),
            (pl.col('DT_REFER').dt.month()).alias('MONTH'),
            (pl.col('DT_REFER').dt.day()).alias('DAY')
        ])
    
    ds.write_dataset(
        df.to_arrow(),
        zone_path,
        format='parquet',
        partitioning=['YEAR', 'MONTH', 'DAY']
    )
    
    
def cvmReadFiles(zone, table):
    zone = str.lower(zone)
    table = str.lower(table)
    zone_path = dataLakePath(zone, table)
    
    table = ds.dataset(zone_path, format='parquet').to_table()
    polars_df = pl.DataFrame(table)
    
    return polars_df