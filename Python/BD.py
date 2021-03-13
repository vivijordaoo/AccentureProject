import pyodbc
from datetime import date, datetime
import pandas as pd

class BD(object):
  __server = 'datazilla.database.windows.net'
  __database = 'datazilla'
  __username = 'datazilla'
  __password = 'gama123456@#$'   
  """
  __server = 'localhost'
  __database = 'Datazilla'
  __username = 'sa'
  __password = '251x2mdlltfd'   
  """
    
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
      self.conectorBD.execute("""CREATE TABLE PAIS
                                  (
                                    ID 				INT				NOT NULL IDENTITY(1, 1),  	--PK
                                    NOME			VARCHAR(100)     NOT NULL,
                                    SLUG			VARCHAR(100)     NULL,
                                    SIGLA           	VARCHAR(2)      NULL,
                                    CONSTRAINT 		PK_PAIS 	PRIMARY KEY (ID)
                                  );""")
        
      self.conectorBD.execute("""CREATE TABLE DADOS_PAISES
                                  (
                                    ID 				INT				NOT NULL IDENTITY(1, 1),  --PK
                                    ID_PAIS			INT			NOT NULL,	 	 --FK PAIS
                                    LAT			DECIMAL			NOT NULL,
                                    LON			DECIMAL 		NOT NULL,
                                    CONFIRMED		INT			NULL,
                                    DEATHS			INT			NULL,
                                    RECOVERED		INT			NULL,
                                    ACTIVE			INT			NULL,
                                    DATE			DATETIME		NOT NULL,
                                    
                                    CONSTRAINT 		PK_ID			PRIMARY KEY (ID),
                                    CONSTRAINT FK_DADOS_PAISES_PAIS 		FOREIGN KEY (ID_PAIS)
                                      REFERENCES PAIS(ID)
                                  );""")

      self.conectorBD.execute("""CREATE TABLE SUMARY_PAISES
                                  (
                                    ID 					INT			NOT NULL IDENTITY(1, 1),  --PK
                                    ID_PAIS				INT			NOT NULL,	 	 --FK PAIS
                                    NEWCONFIRMED		INT			NULL,
                                    TOTALCONFIRMED		INT			NULL,
                                    NEWDEATHS			INT			NULL,
                                    TOTALDEATHS			INT			NULL,
                                    NEWRECOVERED		INT		NULL,
                                    TOTALRECOVERED		INT			NULL,
                                    DATE			DATETIME		NOT NULL,
                                    CONSTRAINT 		PK_SUMARY		PRIMARY KEY (ID),
                                    CONSTRAINT FK_SUMARY_PAIS 		FOREIGN KEY (ID_PAIS)
                                      REFERENCES PAIS(ID)
                                  );""")

      self.conectorBD.execute("""CREATE TABLE LOG 
                                  (
                                    ID 				INT				NOT NULL IDENTITY(1, 1), --PK
                                    DATE		DATETIME	NOT NULL,
                                    DESCRICAO	TEXT		NOT NULL,
                                    CONSTRAINT 	PK_LOG		PRIMARY KEY (ID)
                                  );""")

      self.conectorBD.commit()
    except Exception as error:
      #self.armazena_erros("CRIAR TABELAS", error)
      print(f"{datetime.now().strftime('%H:%M:%S')}: ", f"Except ao criar tabela {error}")
  
  def limpar_tabelas(self):
    try:
      self.conectorBD.execute(f"DELETE FROM DADOS_PAISES;")
      self.conectorBD.execute(f"DELETE FROM SUMARY_PAISES;")
      self.conectorBD.execute(f"DELETE FROM PAIS;")
      self.conectorBD.execute(f"DELETE FROM LOG;")
      self.conectorBD.commit()
    except Exception as error:
      self.armazena_erros(f"{datetime.now().strftime('%H:%M:%S')}: ", "LIMPAR TABELAS", error)

  def limpar_tabelas_DADOSPAISES(self):
    try:
      self.conectorBD.execute(f"DELETE FROM DADOS_PAISES;")
    except Exception as error:
      self.armazena_erros(f"{datetime.now().strftime('%H:%M:%S')}: ", "LIMPAR TABELAS_DADOSPAISES", error)

  def limpar_tabelas_SUMARY_PAISES(self):
    try:
      self.conectorBD.execute(f"DELETE FROM SUMARY_PAISES;")
    except Exception as error:
      self.armazena_erros(f"{datetime.now().strftime('%H:%M:%S')}: ", "LIMPAR TABELAS_SUMARY_PAISES", error)

  def limpar_tabelas_LOG(self):
    try:
      self.conectorBD.execute(f"DELETE FROM LOG;")
    except Exception as error:
      self.armazena_erros(f"{datetime.now().strftime('%H:%M:%S')}: ", "LIMPAR TABELAS_LOG", error)


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
      cont = 0
      #poderia ser executemany, mas por causa da namibia derruba tudo
      for item in val:
        try:
          cursor.execute(sql, item)
          cont = cont + 1
          if cont > 20:
            self.conectorBD.commit()
            cont = 0
                  
        except Exception as error:
          self.armazena_erros(', '.join(str(x) for x in item), error)
          print(f"{datetime.now().strftime('%H:%M:%S')}: "
              f"Inserção com erro {', '.join(str(x) for x in item)} - {error}!\n")
      
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
    self.inserir(sql, campos, df_by_country)

  def armazena_sumary_paises(self, df_sumary):
    campos = {'ID_PAIS':'ID_PAIS', 'NEWCONFIRMED':'NewConfirmed', 'TOTALCONFIRMED':'TotalConfirmed', 'NEWDEATHS':'NewDeaths', 'TOTALDEATHS':'TotalDeaths', 'NEWRECOVERED':'NewRecovered', 'TOTALRECOVERED':'TotalRecovered', 'DATE':'Date'}
    sql = f"INSERT INTO SUMARY_PAISES (ID_PAIS, NEWCONFIRMED, TOTALCONFIRMED, NEWDEATHS, TOTALDEATHS, NEWRECOVERED, TOTALRECOVERED, DATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"     
    self.inserir(sql, campos, df_sumary)

  def armazena_erros(self, item, erro):
    self.conectorBD.execute("INSERT INTO LOG VALUES (GETDATE(), ?)",
                      f"Registro que originou o problema: {item}."
                      f" Informações Técnicas: {str(erro)}")
    self.conectorBD.commit()
    pass

  def execSelect(self, sql : str):
    insertObject = []
    try:
      with self.conectorBD.cursor() as cursor:
        cursor.execute(sql)
        columnNames = [column[0] for column in cursor.description]       
        for record in cursor.fetchall():
            insertObject.append( dict( zip( columnNames , record ) ) )
    except Exception as erro:
      print(f"{datetime.now().strftime('%H:%M:%S')}: "
            f"Erro na consulta {sql} {erro} !\n")
    return insertObject   
  
  def consultaPanoramaCasosConfirmadosTop10(self):
    res = self.execSelect(f"""SELECT TOP 10 PAIS.NOME, SUMARY_PAISES.NEWCONFIRMED
                              FROM PAIS
                              INNER JOIN SUMARY_PAISES
                              ON PAIS.ID = SUMARY_PAISES.ID_PAIS
                              ORDER BY SUMARY_PAISES.NEWCONFIRMED DESC""")
    return pd.DataFrame(res)

  def consultaPanoramaQtdeMortesTop10(self):
    res = self.execSelect(f"""SELECT TOP 10 PAIS.NOME, SUMARY_PAISES.NEWDEATHS
                              FROM PAIS
                              INNER JOIN SUMARY_PAISES
                              ON PAIS.ID = SUMARY_PAISES.ID_PAIS
                              ORDER BY SUMARY_PAISES.NEWDEATHS DESC""")
    return pd.DataFrame(res)    

  def consultaTotalMortesTop10(self):
    res = self.execSelect(f"""SELECT TOP 10 PAIS.NOME, SUMARY_PAISES.TOTALDEATHS
                              FROM PAIS
                              INNER JOIN SUMARY_PAISES
                              ON PAIS.ID = SUMARY_PAISES.ID_PAIS
                              ORDER BY SUMARY_PAISES.TOTALDEATHS DESC""")
    return pd.DataFrame(res)        

  def consultaTotalCasosConfirmadosTop10(self):
    res = self.execSelect(f"""SELECT TOP 10 PAIS.NOME, SUMARY_PAISES.TOTALCONFIRMED
                              FROM PAIS
                              INNER JOIN SUMARY_PAISES
                              ON PAIS.ID = SUMARY_PAISES.ID_PAIS
                              ORDER BY SUMARY_PAISES.TOTALCONFIRMED DESC""")
    return pd.DataFrame(res) 

  def consultaPais(self):
    res = self.execSelect(f"""SELECT DISTINCT id, NOME, SIGLA FROM PAIS;""")
    return pd.DataFrame(res) 

  def consultaSumary(self, id, data):
    res = self.execSelect(f"""SELECT DISTINCT id FROM SUMARY_PAISES WHERE id_pais = {id} and [date] >= CAST( '{data}' AS Date );""")
    return pd.DataFrame(res) 
