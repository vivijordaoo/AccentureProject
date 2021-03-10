import pyodbc
from datetime import date, datetime
import pandas as pd

class BD(object):
  __server = 'datazilla.mariadb.database.azure.com'
  __database = 'datazilla'
  __username = 'gballesteros'
  __password = 'Admin2895'   
  __port= '3306'
  __driver= '{maria}'
  __connString = 'DRIVER='+__driver+';SERVER='+__server+';PORT='+__port+';DATABASE='+__database+';UID='+__username+';PWD='+ __password

  def __init__(self):
    self.conectorBD = self.conecta_BD()

  def conecta_BD(self):
      try:
        print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Conectando ao BD no Azure...")
        conector = pyodbc.connect(self.__connString)
        print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Conexão realizada com sucesso com o BD!")
        return conector
      except Exception as error:
        print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Não foi possível se conectar com o BD...")
        return 0

  def criar_tabelas(self):
    try:
      self.conectorBD.execute("""create table PAIS
                            (
                              id			int 		not null auto_increment, #	pk
                              nome			varchar(50)     not null,
                              slug			varchar(50)     null,
                              sigla           	varchar(2)      null,
                              CONSTRAINT 		pk_pais 	primary key (id)
                            );""")
        
      self.conectorBD.execute("""create table DADOS_PAISES
                            (
                              id			int 			not null auto_increment, # pk
                              id_pais			int			not null,	 	 # fk pais
                              lat			decimal			not null,
                              lon			decimal 		not null,
                              confirmed		int			null,
                              deaths			int			null,
                              recovered		int			null,
                              active			int			null,
                              date			datetime		not null,
                              
                              constraint 		pk_id			primary key (id),
                              constraint fk_dados_paises_pais 		foreign key (id_pais)
                                references pais(id)
                            );""")

      self.conectorBD.execute("""create table log 
                            (
	                            id		int		not null auto_increment, #pk
	                            data		datetime	not null,
	                            descricao	text		not null,
	                            constraint 	pk_log		primary key (id)
                            );""")

      self.conectorBD.commit()
    except Exception as error:
      #self.armazena_erros("CRIAR TABELAS", error)
      print(f"Except ao criar tabela {error}")
  
  def limpar_tabelas(self):
    try:
      self.conectorBD.execute(f"DELETE FROM PAIS;")
      self.conectorBD.execute(f"DELETE FROM DADOS_PAISES;")
      self.conectorBD.commit()
    except Exception as error:
      self.armazena_erros("LIMPAR TABELAS", error)


  def inserir(self, sql : str, campos : dict, df):
    val = []
    #print(df_country)
    for index, row in df.iterrows():
      #print(row)
      temp = []
      for key, valor in campos.items():
        temp.append(row[valor])
      val.append(temp)

    print(val)

    try:
      self.conectorBD.executemany(sql, val)
      #Necessário preencher com os campos da API selecionada
    except Exception as error:
      self.armazena_erros("Armazena paises", error)
    self.conectorBD.commit()
    print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Inserção concluída com sucesso!\n")
    pass

  def armazena_paises(self, df_country):
    campos = {'NOME': 'Country', 'SLUG': 'Slug', 'SIGLA': 'ISO2'}
    sql = "INSERT INTO PAIS(NOME, SLUG, SIGLA) VALUES (?, ?, ?);"
    self.inserir(sql, campos, df_country)

  def armazena_dados_paises(self, df_by_country):
    campos = {'ID_PAIS':'CountryCode', 'LAT':'Lat', 'LON':'Lon', 'CONFIRMED':'Confirmed', 'DEATHS':'Deaths', 'RECOVERED':'Recovered',
    'ACTIVE':'Active', 'DATE':'Date'}
    sql = """INSERT INTO DADOS_PAISES (ID_PAIS, LAT, LON, CONFIRMED, DEATHS, RECOVERED, ACTIVE, DATE)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""
    self.conectorBD.execute("SELECT id, nome FROM PAIS GROUP BY 1,2;")
    for row in self.conectorBD.fetchall():
      df_by_country[df_by_country['Country'].str.lower() == row.nome.lower()]['CountryCode'] = row.id
    self.inserir(sql, campos, df_by_country)

  def armazena_erros(self, item, erro):
    self.conectorBD.execute("INSERT INTO LOG VALUES (GETDATE(), ?)",
                      f"Registro que originou o problema: {item}."
                      f"Informações Técnicas: {str(erro)}")
    self.conectorBD.commit()
    pass