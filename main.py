from Python import retorno_api as API
from Python import BD as conn_BD
from Python import processa_dados as Proc

import pandas as pd
from datetime import datetime
from datetime import date

class Main(object):
    def baixaAPIPAIS(self, doBuffer: bool = False):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando PAIS da API")
        df_country = API.Country().retorna_dataframe(doBuffer)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")
        return df_country
        
    def baixaAPISUMARY(self, doBuffer: bool = False):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando SUMARY da API")
        df_sumary = API.Summary().retorna_dataframe(doBuffer)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")
        return df_sumary

    def baixaAPIBYCONTRY(self, doBuffer: bool = False):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando BY_COUNTRY da API")
        df_by_country = API.By_Country().retorna_dataframe(doBuffer)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")
        return df_by_country


    def baixaAPITodas(self, doBuffer: bool = False):
        if doBuffer:
            print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Dados da API do buffer CSV")
        else:    
            print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Dados da API NO CSV")
        
        df_sumary = self.baixaAPISUMARY(doBuffer)
        df_country = self.baixaAPIPAIS(doBuffer)
        df_by_country = self.baixaAPIBYCONTRY(doBuffer)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")
        return df_sumary, df_country, df_by_country
    
    def processaDFPais(self, df_country):
        print(f"{datetime.now().strftime('%H:%M:%S')} Processando COUNTRY DF")
        df_country = Proc.ProcessaDF().processaDFPais(df_country)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")
        return df_country

    def processaDFDadosPais(self, df_by_country):
        print(f"{datetime.now().strftime('%H:%M:%S')} Processando BY COUNTRY DF")
        df_by_country = Proc.ProcessaDF().processaDFDadosPais(df_by_country)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")
        return df_by_country

    def processaDFSumaryPais(self, df_sumary):
        print(f"{datetime.now().strftime('%H:%M:%S')} Processando SUMARY DF")
        df_sumary = Proc.ProcessaDF().processaDFSumaryPais(df_sumary)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")
        return df_sumary

    def carregaPAISTabela(self, df_country):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Dataframe no PAIS")
        conectorBD = conn_BD.BD()
        conectorBD.armazena_paises(df_country)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def carregaSUMARYTabela(self, df_sumary):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Dataframe no SUMARY_PAISES")
        conectorBD = conn_BD.BD()
        conectorBD.armazena_sumary_paises(df_sumary)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def carregaDADOSPAISESTabela(self, df_by_country):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Dataframe no DADOS_PAISES")
        conectorBD = conn_BD.BD()
        conectorBD.armazena_dados_paises(df_by_country)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def carregaAPICOUNTRY_TabelaPAIS(self, doBuffer: bool = False):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando PAIS da API para TABELA")
        df_country = self.baixaAPIPAIS(doBuffer)
        df_country = self.processaDFPais(df_country)
        self.carregaPAISTabela(df_country)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def carregaAPIBYCOUNTRY_TabelaDadosPaies(self, doBuffer: bool = False):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Dados_PAISES da API para TABELA (PAIS já deve estar POPULADO)")
        df_by_country = self.baixaAPIBYCONTRY(doBuffer)
        df_by_country = self.processaDFDadosPais(df_by_country)
        self.carregaDADOSPAISESTabela(df_by_country)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def carregaAPISUMARY_TabelaSumaryPaises(self, doBuffer: bool = False):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Sumary_PAISES da API para TABELA (PAIS já deve estar POPULADO)")
        df_sumary = self.baixaAPISUMARY(doBuffer)
        df_sumary = self.processaDFSumaryPais(df_sumary)
        self.carregaSUMARYTabela(df_sumary)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def carregaDFTabela(self, doBuffer: bool = False):
        print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Dataframe no BD")
        self.carregaAPICOUNTRY_TabelaPAIS(doBuffer)
        self.carregaAPISUMARY_TabelaSumaryPaises(doBuffer)
        self.carregaAPIBYCOUNTRY_TabelaDadosPaies(doBuffer)
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def carregaDFTabeladoBuffer(self):
        self.carregaDFTabela(True)

    def criaTabela(self):
        print(f"{datetime.now().strftime('%H:%M:%S')} Criando Tabelas no BD")
        conectorBD = conn_BD.BD()
        conectorBD.criar_tabelas()
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")
        
    def limpaTabela(self):
        print(f"{datetime.now().strftime('%H:%M:%S')} Limpando Tabelas no BD")
        conectorBD = conn_BD.BD()
        conectorBD.limpar_tabelas()
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def limpaTabelaDADOSPAISES(self):
        print(f"{datetime.now().strftime('%H:%M:%S')} Limpando Tabelas DADOS_PAISES")
        conectorBD = conn_BD.BD()
        conectorBD.limpar_tabelas_DADOSPAISES()
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def limpaTabelaSUMARYPAISES(self):
        print(f"{datetime.now().strftime('%H:%M:%S')} Limpando Tabelas SUMARY_PAISES")
        conectorBD = conn_BD.BD()
        conectorBD.limpar_tabelas_SUMARY_PAISES()
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def limpaTabelaLOG(self):
        print(f"{datetime.now().strftime('%H:%M:%S')} Limpando Tabelas LOG")
        conectorBD = conn_BD.BD()
        conectorBD.limpar_tabelas_LOG()
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def executaConsulta1(self):
        conectorBD = conn_BD.BD()
        df1 = conectorBD.consultaPanoramaCasosConfirmadosTop10()
        print(f"{datetime.now().strftime('%H:%M:%S')} Panorama diário de quantidade de casos confirmados de COVID-19 dos 10 países do mundo com maiores números.")
        print(df1)
        print("\n\n")
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def executaConsulta2(self):
        conectorBD = conn_BD.BD()
        df2 = conectorBD.consultaPanoramaQtdeMortesTop10()
        print(f"{datetime.now().strftime('%H:%M:%S')} Panorama diário de quantidade de mortes de COVID-19 dos 10 países do mundo com números.")
        print(df2)
        print("\n\n")
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def executaConsulta3(self):
        conectorBD = conn_BD.BD()
        df3 = conectorBD.consultaTotalMortesTop10()
        print(f"{datetime.now().strftime('%H:%M:%S')} Total de mortes por COVID-19 dos 10 países do mundo com maiores números.")
        print(df3)
        print("\n\n")
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def executaConsulta4(self):
        conectorBD = conn_BD.BD()
        df4 = conectorBD.consultaTotalCasosConfirmadosTop10()
        print(f"{datetime.now().strftime('%H:%M:%S')} Total de casos confirmados por COVID-19 dos 10 países do mundo com maiores números.")
        print(df4)
        print("\n\n")
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

    def executaTodasConsultas(self):
        print(f"{datetime.now().strftime('%H:%M:%S')} Executando todas as consultas.")
        self.executaConsulta1()
        self.executaConsulta2()
        self.executaConsulta3()
        self.executaConsulta4()
        print("\n\n")
        print(f"{datetime.now().strftime('%H:%M:%S')} Finalizado... ")

#--------------

def recebeOpcaoUsuario():
    opcao = 0

    print("Digite a opção que deseja executar:\n"
          "===== Consulta =====\n"
          "1 - Exibi TODAS as consultas na Tela\n"
          "2 - Exibi Panorama diário de quantidade de casos confirmados de COVID-19 dos 10 países do mundo com maiores números\n"
          "3 - Panorama diário de quantidade de mortes de COVID-19 dos 10 países do mundo com números\n"
          "4 - Total de mortes por COVID-19 dos 10 países do mundo com maiores números\n"
          "5 - Total de casos confirmados por COVID-19 dos 10 países do mundo com maiores números\n"
          "===== Carga de Dados =====\n"
          "6 - Carregando Dados da API no buffer CSV\n"
          "7 - Carregar Todos os Dados da API nas Tabelas\n"
          "8 - Carregar Todos os Dados do BUFFER nas Tabelas\n"
          "9 - Carregar PAIS na Tabela\n"
          "10 - Carregar Dados de PAISES na Tabela\n"
          "11 - Carregar Sumario de PAISES na Tabela\n"
          "===== Manutenção =====\n"
          "12 - Cria estrutura de tabelas no DB Selecionado\n"
          "13 - Limpar conteúdo das Tabelas\n"
          "14 - Limpar conteúdo DADOS_PAISES\n"
          "15 - Limpar conteúdo SUMARY_PAISES\n"
          "16 - Limpar conteúdo LOG\n"
          "===== FIM =====\n"
          "17 - Sair do Programa\n")

    while opcao < 1 or opcao > 17:
        temp = input("Digite uma opção válida (1 - 17): ")
        if temp.isnumeric():
            opcao = int(temp)
        else:
            opcao = 0    
        if opcao < 1 or opcao > 17:
            print("Opção inválida. Digite novamente")

    return opcao

if __name__ == '__main__':
    opcao = recebeOpcaoUsuario()

    main = Main()
    while opcao >= 1 and opcao < 17:
        if opcao == 1: 
            print("Opção 1 - Exibi TODAS as consultas na Tela\n") 
            main.executaTodasConsultas()                       

        elif opcao == 2: 
            print("Opção 2 - Exibi Panorama diário de quantidade de casos confirmados de COVID-19 dos 10 países do mundo com maiores números\n") 
            main.executaConsulta1()                       

        elif opcao == 3: 
            print("Opção 3 - Panorama diário de quantidade de mortes de COVID-19 dos 10 países do mundo com números\n") 
            main.executaConsulta2()                       

        elif opcao == 4: 
            print("Opção 4 - Total de mortes por COVID-19 dos 10 países do mundo com maiores números\n") 
            main.executaConsulta3()                       

        elif opcao == 5: 
            print("Opção 5 - Total de casos confirmados por COVID-19 dos 10 países do mundo com maiores números\n") 
            main.executaConsulta4()                       
#---------
        elif opcao == 6: 
            print("Opção 6 - Baixar Arquivo Coronavirus COVID19 da API")
            main.baixaAPITodas()

        elif opcao == 7: 
            print("Opção 7 - Carregar Todos os Dados da API nas Tabelas")
            main.carregaDFTabela()

        elif opcao == 8: 
            print("Opção 8 - Carregar Todos os Dados do BUFFER nas Tabelas")
            main.carregaDFTabeladoBuffer()

        elif opcao == 9: 
            print("Opção 9 - Carregar PAIS na Tabela")
            main.carregaAPICOUNTRY_TabelaPAIS()

        elif opcao == 10: 
            print("Opção 10 - Carregar Dados de PAISES na Tabela (BUFFER)")
            main.carregaAPIBYCOUNTRY_TabelaDadosPaies(True)

        elif opcao == 11: 
            print("Opção 11 - Carregar Sumario de PAISES na Tabela")
            main.carregaAPISUMARY_TabelaSumaryPaises()
#---------

        elif opcao == 12: 
            print("Opção 12 - Cria estrutura de tabelas no DB Selecionado")
            main.criaTabela()

        elif opcao == 13: 
            print("Opção 13 - Limpar conteúdo das Tabelas")
            main.limpaTabela()

        elif opcao == 14: 
            print("Opção 14 - Limpar conteúdo DADOS_PAISES")
            main.limpaTabelaDADOSPAISES()

        elif opcao == 15: 
            print("Opção 15 - Limpar conteúdo SUMARY_PAISES")
            main.limpaTabelaSUMARYPAISES()

        elif opcao == 16: 
            print("Opção 16 - Limpar conteúdo LOG")
            main.limpaTabelaLOG()

        else: #sair do programa
            print("Você saiu do programa. Obrigado por usar...\n")
            opcao = 17

        if opcao != 17:
            opcao = recebeOpcaoUsuario()
