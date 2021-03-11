import requests
import json
import pandas as pd
from datetime import datetime
from datetime import date
import os.path
class RetriveAPI(object):
    __url = str
    __campos = {}
    __payload = {}
    __headers= {}
    __raiz = str

    def __init__(self, url : str, campos : dict, caminho_raiz: str = ''):
        self.__url = url
        self.__campos = campos
        self.__raiz = caminho_raiz

    def get_url(self):
        return self.__url

    def set_url(self, url):
        self.__url = url

    def retorna_dataframe(self):
        df = pd.DataFrame()
        try:
            response = requests.request("GET", self.__url, headers=self.__headers, data =self.__payload)
            raw_json = response.json()
    
            #print(raw_json)
            tjson = {}
            if self.__raiz != '':
                tjson = raw_json[self.__raiz].copy()
            else: 
                tjson = raw_json.copy()

            val = {}
            for key in self.__campos.keys():
                val[key] = []

            #print(val)
                    
            for item in tjson:
                #print(item)
                for key, valor in self.__campos.items():
                    #print(valor)
                    if type(valor) != list:
                        #print(f"{valor} => ({item[valor]})")
                        val[key].append(item[valor])
                    else:
                        x = item.copy()
                        for tt in range(len(valor)):
                            x = x.get(valor[tt])   
                        #print(f" ({x})")
                        val[key].append(x)
                #print(val)


            for key in self.__campos.keys():    
                df[key] = pd.Series(val[key], dtype='object')

            df = df.fillna(0)
        except Exception as error:
            print(f"{datetime.now().strftime('%H:%M:%S')}: "
                f"Falha ao obter da API {error}!\n")
            raise Exception(error)
            
        return df

class Summary(object):
    __url = "https://api.covid19api.com/summary"
    __campos = {'Country': 'Country', 'CountryCode': 'CountryCode', 'NewConfirmed': 'NewConfirmed', 'TotalConfirmed': 'TotalConfirmed', 'NewDeaths': 'NewDeaths', 'TotalDeaths': 'TotalDeaths', 'NewRecovered': 'NewRecovered', 'TotalRecovered': 'TotalRecovered', 'Date' : 'Date'}
    __myRetrive = RetriveAPI(__url,  __campos, 'Countries')

    def retorna_dataframe(self, doCache : bool = False):
        name = r'Python/backup_csv/summary.csv'
        if doCache:
            df = pd.read_csv(name)
            df = df.fillna(0)
        else:  
            try:
                df = self.__myRetrive.retorna_dataframe()
                df.to_csv(name)
            except Exception as error:
                print(f"{datetime.now().strftime('%H:%M:%S')}: "
                    f"Cerregando do Buffer {error}!\n")
                if os.path.isfile(name):
                    df = pd.read_csv(name)    
                else:
                    df = pd.DataFrame()

        try:
            df = df.astype({'CountryCode': str, 'NewConfirmed': int, 'TotalConfirmed': int, 'NewDeaths': int, 'TotalDeaths': int, 'NewRecovered': int, 'TotalRecovered': int})
        except Exception as error:
            print(f"{datetime.now().strftime('%H:%M:%S')}: "
                f"Erro ao aplicar o Schema, dados inconsistentes {error}!\n")
        return df

class Country(object):
    __url = "https://api.covid19api.com/countries"
    __campos = {'Country': 'Country', 'Slug': 'Slug', 'ISO2': 'ISO2'}
    __myRetrive = RetriveAPI(__url,  __campos)

    def retorna_dataframe(self, doCache : bool = False):
        name = r'Python/backup_csv/country.csv'
        if doCache:
            df = pd.read_csv(name)
            df = df.fillna(0)
        else:  
            try:
                df = self.__myRetrive.retorna_dataframe()
                df.to_csv(name)
            except Exception as error:
                print(f"{datetime.now().strftime('%H:%M:%S')}: "
                    f"Cerregando do Buffer {error}!\n")
                if os.path.isfile(name):
                    df = pd.read_csv(name)    
                else:
                    df = pd.DataFrame()
        
        try:
            df = df.astype({'Country': str, 'Slug': str, 'ISO2': str})
        except Exception as error:
            print(f"{datetime.now().strftime('%H:%M:%S')}: "
                f"Erro ao aplicar o Schema, dados inconsistentes {error}!\n")
        
        return df
    
class All_Data(object):
    __url = "https://api.covid19api.com/all"
    __campos = {'Country': 'Country', 'Country': 'Country', 'CountryCode': 'CountryCode', 'Lat': 'Lat', 'Lon': 'Lon', 'Confirmed':'Confirmed', 'Deaths': 'Deaths', 'Recovered': 'Recovered', 'Active': 'Active', 'Date': 'Date'}
    __myRetrive = RetriveAPI(__url,  __campos)

    def retorna_dataframe(self):
        df = self.__myRetrive.retorna_dataframe()
        df.to_csv(r'Python/backup_csv/country.csv')
        return df
class By_Country(object):
    class_country = Country()
    __campos = {"Country" : "Country", "CountryCode": "CountryCode", "Province": "Province", "City": "City", "CityCode": "CityCode", "Lat": "Lat", "Lon": "Lon", "Confirmed": "Confirmed", "Deaths": "Deaths", "Recovered": "Recovered", "Active": "Active", "Date": "Date"}
    
    def retorna_dataframe(self, doCache : bool = False):
        df = self.class_country.retorna_dataframe()
        #print(df)
        result = pd.DataFrame()

        for pais in df['Slug']:
            df_int = pd.DataFrame()
            name = f'Python/backup_csv/{pais}.csv'
            if doCache:
                df_int = pd.read_csv(name)
                df_int = df_int.fillna(0)
                result = result.append(df_int)
            else:    
                try:
                    __url = f"https://api.covid19api.com/total/country/{pais}"
                    #print(__url)             
                    __myRetrive = RetriveAPI(__url,  self.__campos)
                    df_int = __myRetrive.retorna_dataframe()
                    if not df_int.empty:
                        df_int.to_csv(name)
                    else:
                        df_int = pd.read_csv(name)
                    result = result.append(df_int)

                except Exception as error:
                    print(f"{datetime.now().strftime('%H:%M:%S')}: "
                        f"Cerregando do Buffer {error}!\n")
                    if os.path.isfile(name):
                        df_int = pd.read_csv(name)    
                #print(df_int)
            
        #print(result)
        #renumera a primera coluna
        try:
            result = result.astype({'CountryCode': int, "Confirmed": int, "Deaths": int, "Recovered": int, "Active": int})
        except Exception as error:
            print(f"{datetime.now().strftime('%H:%M:%S')}: "
                f"Erro ao aplicar o Schema, dados inconsistentes {error}!\n")
  
        result.to_csv(f'Python/backup_csv/bycontry.csv')
        #print(result)
        return result

if __name__ == '__main__':
    """
    campos = {'Country': 'Country', 'CountryCode': 'CountryCode', 'NewConfirmed': 'NewConfirmed', 'TotalConfirmed': 'TotalConfirmed', 'NewDeaths': 'NewDeaths', 'TotalDeaths': 'TotalDeaths', 'NewRecovered': 'NewRecovered', 'TotalRecovered': 'TotalRecovered', 'Date' : 'Date'}

    test = RetriveAPI("https://api.covid19api.com/summary", campos, 'Countries')
    df = test.retorna_dataframe()
    print(df)

    campos = {'Country': 'Country', 'Slug': 'Slug', 'ISO2': 'ISO2'}

    test = RetriveAPI("https://api.covid19api.com/countries", campos)
    df = test.retorna_dataframe()
    print(df)

    """
    sumary = Summary()
    df = sumary.retorna_dataframe()
    print(df)
    print(df.keys())    

    country = Country()
    df = country.retorna_dataframe()
    print(df)    
    print(df.keys())    
    
    bycountry = By_Country()
    df = bycountry.retorna_dataframe()
    #problema .. a coluna ID não foi "unificada"
    print(df)    
    print(df.keys())    







