
import requests
import json
import pandas as pd

class RetriveAPI:
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

    def dados_raw(self):
        response = requests.request("GET", self.__url, headers=self.__headers, data =self.__payload)
        return response
    
    def retorna_dataframe(self):
        raw_json = self.dados_raw().json()
        df = pd.DataFrame()
        
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
            df[key] = pd.Series(val[key])

        #df.to_csv(r'backup_csv/summary.csv')
        return df

class Summary:
    __url = "https://api.covid19api.com/summary"
    __campos = {'Country': 'Country', 'CountryCode': 'CountryCode', 'NewConfirmed': 'NewConfirmed', 'TotalConfirmed': 'TotalConfirmed', 'NewDeaths': 'NewDeaths', 'TotalDeaths': 'TotalDeaths', 'NewRecovered': 'NewRecovered', 'TotalRecovered': 'TotalRecovered', 'Date' : 'Date'}
    __myRetrive = RetriveAPI(__url,  __campos, 'Countries')

    def retorna_dataframe(self):
        return self.__myRetrive.retorna_dataframe()
    
class Country:
    __url = "https://api.covid19api.com/countries"
    __campos = {'Country': 'Country', 'Slug': 'Slug', 'ISO2': 'ISO2'}
    __myRetrive = RetriveAPI(__url,  __campos)

    def retorna_dataframe(self):
        return self.__myRetrive.retorna_dataframe()

class All_Data:
    __url = "https://api.covid19api.com/all"
    __campos = {'Country': 'Country', 'Country': 'Country', 'CountryCode': 'CountryCode', 'Lat': 'Lat', 'Lon': 'Lon', 'Confirmed':'Confirmed', 'Deaths': 'Deaths', 'Recovered': 'Recovered', 'Active': 'Active', 'Date': 'Date'}
    __myRetrive = RetriveAPI(__url,  __campos)

    def retorna_dataframe(self):
        return self.__myRetrive.retorna_dataframe()
class By_Country:
    
    class_country = Country()
    __url = "https://api.covid19api.com/total/country/{0}"
    __payload = {}
    __headers= {}
    __country_data = {}
    __data_by_country = {}
    __country_ok = []
    __list_countries = list(class_country.retorna_dataframe()["Country"])

    def get_url(self):
        return self.__url
    def set_url(self, url):
        self.__url = url

    def dados_raw(self):
        for country in range(len(self.__list_countries)):
            if requests.get(self.__url.format(self.__list_countries[country])).status_code == 200:
                self.__country_data[self.__list_countries[country]] = requests.request("GET", self.__url.format(self.__list_countries[country]),
                headers=self.__headers, data = self.__payload)
                self.__country_ok.append(self.__list_countries[country])
        return self.__country_data

    def retorna_dataframe(self):
        Country = []
        CountryCode = []
        Province = []
        City = []
        CityCode = []
        Lat = []
        Lon = []
        Confirmed = []
        Deaths = []
        Recovered = []
        Active = []
        Date = []

        raw = self.dados_raw()

        for country in self.__country_ok:
            self.__data_by_country[country] = raw[country].json()

        for country in self.__country_ok:
            for pais in range(len(self.__data_by_country[country])):
                Country.append(self.__data_by_country[country][pais]['Country'])
                CountryCode.append(self.__data_by_country[country][pais]['CountryCode'])
                Province.append(self.__data_by_country[country][pais]['Province'])
                City.append(self.__data_by_country[country][pais]['City'])
                CityCode.append(self.__data_by_country[country][pais]['CityCode'])
                Lat.append(self.__data_by_country[country][pais]['Lat'])
                Lon.append(self.__data_by_country[country][pais]['Lon'])
                Confirmed.append(self.__data_by_country[country][pais]['Confirmed'])
                Deaths.append(self.__data_by_country[country][pais]['Deaths'])
                Recovered.append(self.__data_by_country[country][pais]['Recovered'])
                Active.append(self.__data_by_country[country][pais]['Active'])
                Date.append(self.__data_by_country[country][pais]['Date'])

        df = pd.DataFrame()
        df["Country"] = pd.Series(Country)
        df["CountryCode"] = pd.Series(CountryCode)
        df["Province"] = pd.Series(Province)
        df["City"] = pd.Series(City)
        df["CityCode"] = pd.Series(CityCode)
        df["Lat"] = pd.Series(Lat)
        df["Lon"] = pd.Series(Lon)
        df["Confirmed"] = pd.Series(Confirmed)
        df["Deaths"] = pd.Series(Deaths)
        df["Recovered"] = pd.Series(Recovered)
        df["Active"] = pd.Series(Active)
        df["Date"] = pd.Series(Date, dtype='datetime64[ns]')

        df.to_csv(r'backup_csv/by_country.csv')

        return df

if __name__ == '__main__':
    campos = {'Country': 'Country', 'CountryCode': 'CountryCode', 'NewConfirmed': 'NewConfirmed', 'TotalConfirmed': 'TotalConfirmed', 'NewDeaths': 'NewDeaths', 'TotalDeaths': 'TotalDeaths', 'NewRecovered': 'NewRecovered', 'TotalRecovered': 'TotalRecovered', 'Date' : 'Date'}

    test = RetriveAPI("https://api.covid19api.com/summary", campos, 'Countries')
    df = test.retorna_dataframe()
    print(df)

    sumary = Summary()
    df = sumary.retorna_dataframe()
    print(df)

    campos = {'Country': 'Country', 'Slug': 'Slug', 'ISO2': 'ISO2'}

    test = RetriveAPI("https://api.covid19api.com/countries", campos)
    df = test.retorna_dataframe()
    print(df)

    country = Country()
    df = country.retorna_dataframe()
    print(df)    

    







