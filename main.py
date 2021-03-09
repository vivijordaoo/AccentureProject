from Python import retorno_api as API
from Python import BD as conn_BD

import pandas as pd
from datetime import datetime
from datetime import date

df_sumary = pd.DataFrame()
df_country = pd.DataFrame()
df_by_country = pd.DataFrame()

def baixaArquivoAPI():
    print(f"{datetime.now().strftime('%H:%M:%S')} Carregando Dataframe")
    df_sumary = API.Summary().retorna_dataframe()
    df_country = API.Country().retorna_dataframe()
    df_by_country = API.By_Country().retorna_dataframe()

    #print(df_country)

def carregaDFTabela():
    conectorBD = conn_BD.BD()
    print(conectorBD)

def recebeOpcaoUsuario():
    opcao = "0"

    print("Digite a opção que deseja executar:\n"
          "1 - Baixar Arquivo Coronavirus COVID19 da API\n"
          "2 - Cria estrutura de tabelas no DB Selecionado\n"
          "3 - Limpar conteúdo das Tabelas\n"
          "5 - Carregar da API nas Tabelas\n"
          "6 - Selecionar Banco de Dados\n"
          "7 - Exibir consulta na Tela\n"
          "8 - Sair do Programa\n")

    while opcao < "1" or opcao > "8":
        opcao = input("Digite uma opção válida (1 - 8): ")
        if opcao < "1" or opcao > "8":
            print("Opção inválida. Digite novamente")

    return opcao

if __name__ == '__main__':
    opcao = recebeOpcaoUsuario()

    while opcao >= "1" and opcao <= "7":
        if opcao == "1": #1 - Baixar Arquivo Coronavirus COVID19 da API
            print("Opção 1 - Baixar Arquivo Coronavirus COVID19 da API\n") 
            baixaArquivoAPI()                       

        elif opcao == "2": #2 - Cria estrutura de tabelas no DB Selecionado
            print("Opção 2 - Cria estrutura de tabelas no DB Selecionado")
            pass

        elif opcao == "3": #3 - Limpar conteúdo das Tabelas
            print("Opção 3 - Limpar conteúdo das Tabelas")
            pass

        elif opcao == "5": #5 - Carregar da API nas Tabelas
            print("Opção 5 - Carregar da API nas Tabelas")
            carregaDFTabela()
        
        elif opcao == "6": #6 - Selecionar Banco de Dados
            print("Opção 6 - Selecionar Banco de Dados")
            pass
        
        elif opcao == "7": #7 - Exibir consulta na Tela
            print("Opção 7 - Exibir consulta na Tela")
            pass

        else: #sair do programa
            print("Você saiu do programa. Obrigado por usar...\n")
            opcao = "8"

        if opcao != "8":
            opcao = recebeOpcaoUsuario()
