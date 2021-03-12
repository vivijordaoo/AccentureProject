from datetime import date, datetime
from . import BD as conn_BD
import pandas as pd

class ProcessaDF(object):

    def processaDFPais(self, df_country):
        # tira os paises que já existem
        return df_country

    def processaDFDadosPais(self, df_by_country):
        df_country = conn_BD.BD().consultaPais()
        for index, row in df_country.iterrows():
            #print(row[0], row[1].lower())
            df_by_country.loc[df_by_country['Country'].str.lower() == row[1].lower(), 'CountryCode'] = row[0]
        #!!!!!  
        # Grava as linhas q não encontrou pais no log
        #Remove as linhas sem ID (que não encontrou pais)
        #Remove datas que já existem
        df_by_country = df_by_country[df_by_country["CountryCode"] != 0]
        #print(df_by_country)
        return df_by_country    

    def processaDFSumaryPais(self, df_sumary):
        df_country = conn_BD.BD().consultaPais()
        for index, row in df_country.iterrows():
            #print(row[1].lower())
            df_sumary.loc[df_sumary['Country'].str.lower() == row[1].lower(), 'ID_PAIS'] = row[0]
        #!!!!!  
        # Grava as linhas q não encontrou pais no log
        #Remove as linhas sem ID (que não encontrou pais)
        #remove datas que já existem
        #print(df_sumary)
        df_sumary = df_sumary.fillna(0)
        df_sumary = df_sumary.astype({'ID_PAIS': int})
        #print(df_sumary)
        df_sumary = df_sumary[df_sumary["ID_PAIS"] != 0]
        #print(df_sumary)
    
        return df_sumary        