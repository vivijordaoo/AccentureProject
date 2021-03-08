import requests
import json
import pandas as pd

class Summary:
    __url_summary = "https://api.covid19api.com/summary"
    __payload = {}
    __headers= {}

    def get_url(self):
        return self.__url_summary
    def set_url(self, url):
        self.__url_summary = url

    def dados_raw(self):
        response = requests.request("GET", self.__url_summary, headers=self.__headers, data =self.__payload)
        return response

    def retorna_dataframe(self):
        raw_json = self.dados_raw().json()
        df = pd.DataFrame()
        Country = []
        CountryCode = []
        NewConfirmed = []
        TotalConfirmed = []
        NewRecovered = []
        TotalRecovered = []
        Date = []

        for pais in range(len(raw_json['Countries'])):
            Country.append(raw_json["Countries"][pais]['Country'])
            CountryCode.append(raw_json["Countries"][pais]['CountryCode'])
            NewConfirmed.append(raw_json["Countries"][pais]['NewConfirmed'])
            TotalConfirmed.append(raw_json["Countries"][pais]['TotalConfirmed'])
            NewRecovered.append(raw_json["Countries"][pais]['NewRecovered'])
            TotalRecovered.append(raw_json["Countries"][pais]['TotalRecovered'])
            Date.append(raw_json["Countries"][pais]['Date'])        

        df['Country'] = pd.Series(Country)
        df['CountryCode'] = pd.Series(CountryCode)
        df['NewConfirmed'] = pd.Series(NewConfirmed)
        df['TotalConfirmed'] = pd.Series(TotalConfirmed)
        df['NewRecovered'] = pd.Series(NewRecovered)
        df['TotalRecovered'] = pd.Series(TotalRecovered)
        df['Date'] = pd.Series(Date, dtype='datetime64[ns]') 

        return df

class Country:
    __url_summary = "https://api.covid19api.com/countries"
    __payload = {}
    __headers= {}

    def get_url(self):
        return self.__url_summary
    def set_url(self, url):
        self.__url_summary = url

    def dados_raw(self):
        response = requests.request("GET", self.__url_summary, headers=self.__headers, data =self.__payload)
        return response

    def retorna_dataframe(self):
        raw_json = self.dados_raw().json()
        df = pd.DataFrame()
        Country = []
        Slug = []
        ISO2 = []

        for pais in range(len(raw_json)):
            Country.append(raw_json[pais]['Country'])
            Slug.append(raw_json[pais]['Slug'])
            ISO2.append(raw_json[pais]['ISO2'])      

        df['Country'] = pd.Series(Country)
        df['Slug'] = pd.Series(Slug)
        df['ISO2'] = pd.Series(ISO2)

        return df

class All_Data:
    __url = "https://api.covid19api.com/all"
    __payload = {}
    __headers= {}

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
        Country = []
        CountryCode = []
        Lat = []
        Lon = []
        Confirmed = []
        Deaths = []
        Recovered = []
        Active = []
        Date = []

        for pais in range(len(raw_json)):
            Country.append(raw_json[pais]['Country'])
            CountryCode.append(raw_json[pais]['CountryCode'])
            Lat.append(raw_json[pais]['Lat'])
            Lon.append(raw_json[pais]['Lon'])
            Confirmed.append(raw_json[pais]['Confirmed'])
            Deaths.append(raw_json[pais]['Deaths'])
            Recovered.append(raw_json[pais]['Recovered'])
            Active.append(raw_json[pais]['Active'])
            Date.append(raw_json[pais]['Date'])
    

        df['Country'] = pd.Series(Country)
        df['CountryCode'] = pd.Series(CountryCode)
        df['Lat'] = pd.Series(Lat)
        df['Lon'] = pd.Series(Lon)
        df['Confirmed'] = pd.Series(Confirmed)
        df['Deaths'] = pd.Series(Deaths)
        df['Recovered'] = pd.Series(Recovered)
        df['Active'] = pd.Series(Active)
        df['Date'] = pd.Series(Date)

        return df







