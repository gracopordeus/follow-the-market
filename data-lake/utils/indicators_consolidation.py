import polars as pl
from utils.request_cvm_ciasabertas import (
    read_files_from_lake,
)

def consolidate_accounts():
    
    """
    Transform financial indicators data from different reports into a unified format.

    Returns:
    polars.DataFrame: A DataFrame with transformed financial indicators.
    """
    
    CONTAS = {
        'RESULTADO':{
            'CODIGO':{
                'RECEITA':['3.01']  
            },
            'DESCRICAO':{
                'LUCRO':[
                    'lucro/prejuízo consolidado do período',
                    'lucro ou prejuízo líquido consolidado do período',
                    'lucro ou prejuízo líquido do período'
                    ]
            }
        },
        'ATIVO':{
            'CODIGO':{
                'ATIVO_TOTAL':['1'],
                'ATIVO_CIRCULANTE':['1.01']
            }
        },  
        'PASSIVO':{
            'CODIGO':{
                'PASSIVO_TOTAL':['2'],
                'PASSIVO_CIRCULANTE':['2.01']
            },
            'DESCRICAO':{
                'PATRIMONIO_LIQUIDO':[
                    'patrimônio líquido',
                    'patrimônio líquido consolidado'
                    ],
            }
        }
    }


    list_df = []

    for document in CONTAS.keys():
    
        if document == 'RESULTADO':
            report = (
                read_files_from_lake(zone='trusted', table='itr/dre')
                .with_columns(CONTA = pl.lit('na'))
            )
        elif document == 'ATIVO':
            report = (
                read_files_from_lake(zone='trusted', table='itr/bpa')
                .with_columns(CONTA = pl.lit('na'))
            )
        elif document == 'PASSIVO':
            report = (
                read_files_from_lake(zone='trusted', table='itr/bpp')
                .with_columns(CONTA = pl.lit('na'))
            )
        
        for column in CONTAS[document].keys():
            
            if column == 'CODIGO':
                report_column = 'CD_CONTA'
            elif column == 'DESCRICAO':
                report_column = 'DS_CONTA'
                
            for account in CONTAS[document][column].keys():
                
                value = CONTAS[document][column][account]
                
                if value == []:
                    pass
                else:
                    try:
                        df = (
                            report
                            .with_columns(
                                CONTA = pl.when(pl.col(report_column).str.to_lowercase().str.strip_chars().is_in(value))
                                .then(pl.lit(account))
                                .otherwise(pl.col('CONTA'))
                            )
                            .filter(pl.col('CONTA')!='na')
                            .unique()
                        )
                        list_df.append(df)
                    except Exception as e:
                        continue
                    
        polars_df = (
            pl.concat(list_df)
            .pivot(
                index=['DT_REFER', 'CNPJ', 'CD_CVM'],
                columns='CONTA',
                values='VL_CONTA'
            )
        )
    
    return polars_df