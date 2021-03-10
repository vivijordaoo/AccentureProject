import pyodbc
from datetime import date, datetime
import pandas as pd

class BD(object):
  __server = 'localhost'
  __database = 'Datazilla'
  __username = 'sa'
  __password = '251x2mdlltfd'   
  __port= '1433'
  __driver= '{SQL Server}'
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
        print(error)
        return 0

  def criar_tabelas(self):
    try:
      self.conectorBD.execute("""create table PAIS
                            (
                              id			int 		not null identity(1,1),
                              nome			varchar(100)     not null,
                              slug			varchar(100)     null,
                              sigla           	varchar(2)      null,
                              CONSTRAINT 		pk_pais 	primary key (id)
                            );""")
        
      self.conectorBD.execute("""create table DADOS_PAISES
                            (
                              id			int 			not null identity(1,1),
                              id_pais			int			not null,
                              lat			decimal			not null,
                              lon			decimal 		not null,
                              confirmed		int			null,
                              deaths			int			null,
                              recovered		int			null,
                              active			int			null,
                              date			date		not null,
                              
                              constraint 		pk_id			primary key (id),
                              constraint fk_dados_paises_pais 		foreign key (id_pais)
                                references pais(id)
                            );""")

      self.conectorBD.execute("""create table log 
                            (
	                            id		int		not null identity(1,1),
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

    #print(val)

    with self.conectorBD.cursor() as cursor:
      #poderia ser executemany, mas por causa da namibia derruba tudo
      for item in val:
        try:
          cursor.execute(sql, item)
        except Exception as error:
          self.armazena_erros(', '.join(str(x) for x in item), error)
          #print(f"{datetime.now().strftime('%H:%M:%S')}: "
          #    f"Inserção com erro {error}!\n")
      
      self.conectorBD.commit()
    print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Inserção concluída com sucesso!\n")
    pass

  def armazena_paises(self, df_country):
    campos = {'NOME': 'Country', 'SLUG': 'Slug', 'SIGLA': 'ISO2'}
    sql = f"INSERT INTO PAIS(NOME, SLUG, SIGLA) VALUES (?, ?, ?);"
    self.inserir(sql, campos, df_country)

  def armazena_dados_paises(self, df_by_country):
    campos = {'ID_PAIS':'CountryCode', 'LAT':'Lat', 'LON':'Lon', 'CONFIRMED':'Confirmed', 'DEATHS':'Deaths', 'RECOVERED':'Recovered',
    'ACTIVE':'Active', 'DATE':'Date'}
    sql = f"INSERT INTO DADOS_PAISES (ID_PAIS, LAT, LON, CONFIRMED, DEATHS, RECOVERED, ACTIVE, DATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
    with self.conectorBD.cursor() as cursor:
      cursor.execute("SELECT DISTINCT id, nome FROM PAIS;")
      for row in cursor.fetchall():
        #print(row[0], row[1].lower())
        df_by_country.loc[df_by_country['Country'].str.lower() == row[1].lower(), 'CountryCode'] = row[0]
      #!!!!!  
      #Remove as linhas sem ID (que não encontrou pais)
      df_by_country = df_by_country[df_by_country["CountryCode"] != 0]
      #print(df_by_country)
      
      self.inserir(sql, campos, df_by_country)
      
  def armazena_erros(self, item, erro):
    self.conectorBD.execute("INSERT INTO LOG VALUES (GETDATE(), ?)",
                      f"Registro que originou o problema: {item}."
                      f" Informações Técnicas: {str(erro)}")
    self.conectorBD.commit()
    pass