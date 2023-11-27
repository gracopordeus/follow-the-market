from utils.request_cvm_ciasabertas import (
    download_files,
    read_files_from_landing,
    write_files_to_lake,
    read_files_from_lake,
    trasnform_trusted_accounts,
    trasnform_trusted_tickers,
    transform_active_companies,
    trasnform_refined_accounts
)

DOCUMENTS = ['itr', 'fca']
ITR_REPORTS = ['bpa', 'bpp', 'dre']
FCA_REPORTS = ['valor_mobiliario']

# Downloading to Landing
for DOCUMENT in DOCUMENTS:
    download_files(DOCUMENT)


# Landing > Raw > Trusted
for DOCUMENT in DOCUMENTS:
    if DOCUMENT == 'itr':
        REPORTS = ITR_REPORTS
    elif DOCUMENT == 'fca':
        REPORTS = FCA_REPORTS
        
    for REPORT in REPORTS:
        
        DIR = DOCUMENT+'/'+REPORT
        
        df = read_files_from_landing(DOCUMENT, REPORT)
        
        write_files_to_lake(df, 'raw', DIR)
        
        df = read_files_from_lake('raw', DIR)
        
        if DOCUMENT == 'itr':
            df = trasnform_trusted_accounts(df)
        elif DOCUMENT == 'fca':
            df = trasnform_trusted_tickers(df)
        
        write_files_to_lake(df, 'trusted', DIR)


# Refined
df = transform_active_companies()
write_files_to_lake(df, 'refined', 'active_companies')

for REPORT in ITR_REPORTS:
    table = 'itr/'+REPORT
    
    refined_accounts = trasnform_refined_accounts('itr/bpa')
    
    write_files_to_lake(
        df = refined_accounts,
        zone = 'refined',
        table = REPORT
    )