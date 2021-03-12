from datetime import date, datetime
from . import BD as conn_BD
import pandas as pd

class ProcessaDF(object):

    def processaDFPais(self, df_country):
        # tira os paises que já existem
        df_ja_country = conn_BD.BD().consultaPais()
        for index, row in df_ja_country.iterrows():
            #print(row['id'], row['NOME'].lower(), row['SIGLA'].lower())
            #df_country.loc[df_country['ISO2'].str.lower() == row['SIGLA'].lower(), 'jaTem'] = 1
            df_country.loc[df_country['Country'].str.lower() == row['NOME'].lower(), 'jaTem'] = 1
        df_country = df_country[df_country["jaTem"] != 1]
        #print(df_country)
        return df_country

    def processaDFDadosPais(self, df_by_country):
        df_country = conn_BD.BD().consultaPais()
        for index, row in df_country.iterrows():
            #print(row[0], row[1].lower())
            df_by_country.loc[df_by_country['Country'].str.lower() == row['NOME'].lower(), 'CountryCode'] = row['id']
        #!!!!!  
        # Grava as linhas q não encontrou pais no log
        #Remove as linhas sem ID (que não encontrou pais)
        #Remove datas que já existem
        df_by_country = df_by_country[df_by_country["CountryCode"] != 0]
        #print(df_by_country)
        return df_by_country    

    def processaDFSumaryPais(self, df_sumary):
        db = conn_BD.BD()
        df_country = db.consultaPais()
        for index, row in df_country.iterrows():
            #print(row[1].lower())
            df_sumary.loc[df_sumary['Country'].str.lower() == row['NOME'].lower(), 'ID_PAIS'] = row['id']

        #remove dados se o dia já subiu pro DB
        for index, row in df_sumary.iterrows():
            tdate = row['Date'][:10] + " 00:00:00"
            #print(row['Country'], row['Date'], tdate)
            df = db.consultaSumary(row['ID_PAIS'], tdate)
            if not df.empty:
                df_sumary.loc[df_sumary['Country'].str.lower() == row['Country'].lower(), 'jaTem'] = 1

        if 'jaTem' in df_sumary.keys():        
            df_sumary = df_sumary[df_sumary['jaTem'] != 1]
        #print(df_sumary)
                
        #!!!!!  
        # Grava as linhas q não encontrou pais no log
        #Remove as linhas sem ID (que não encontrou pais)
        df_sumary = df_sumary.fillna(0)
        df_sumary = df_sumary.astype({'ID_PAIS': int})
        #print(df_sumary)
        df_sumary = df_sumary[df_sumary["ID_PAIS"] != 0]
        #print(df_sumary)
        return df_sumary        