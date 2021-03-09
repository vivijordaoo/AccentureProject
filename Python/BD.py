import pyodbc
from datetime import date, datetime

class BD(object):
  def __init__(self):
    self.conectorBD = self.conecta_BD()

  def conecta_BD(self):
      try:
        print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Conectando ao BD no Azure...")
        conector = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                    'Server=sqlserver23810.database.windows.net;'
                    'Database=projeto_COVID19;'
                    'UID=sqluser;'
                    'PWD=Licesa@123;')
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
      self.conectorBD.commit()
    except Exception as error:
      self.armazena_erros("CRIAR TABELAS", error)
  
  def limpar_tabelas(self):
    try:
      self.conectorBD.execute(f"DELETE FROM PAIS;")
      self.conectorBD.execute(f"DELETE FROM DADOS_PAISES;")
      self.conectorBD.commit()
    except Exception as error:
      self.armazena_erros("LIMPAR TABELAS", error)


  def armazena_paises(self, conectorBD, conectorAPI):
    for item in conectorAPI:
      try:
        conectorBD.execute("INSERT INTO PAIS VALUES (?, ?)") 
        #Necessário preencher com os campos da API selecionada
      except Exception as error:
        self.armazena_erros(item, error)
    conectorBD.commit()
    print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Inserção concluída com sucesso!\n")
    pass

  def armazena_casos_confirmados(self, conectorBD, conectorAPI):
    for item in conectorAPI:
      try:
        conectorBD.execute("INSERT INTO CASOS_CONFIRMADOS VALUES (?, ?, ?)")
        #Necessário preencher com os campos da API selecionada
      except Exception as error:
        self.armazena_erros(item, error)
    conectorBD.commit()
    print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Inserção concluída com sucesso!\n")
    pass

  def armazena_mortes(self, conectorBD, conectorAPI):
    for item in conectorAPI:
      try:
        conectorBD.execute("INSERT INTO MORTES VALUES (?, ?, ?)")
        #Necessário preencher com os campos da API selecionada
      except Exception as error:
        self.armazena_erros(item, error)
    conectorBD.commit()
    print(f"{datetime.now().strftime('%H:%M:%S')}: "
          f"Inserção concluída com sucesso!")
    pass

  def armazena_erros(self, item, erro):
    self.conectorBD.execute("INSERT INTO LOG VALUES (GETDATE(), ?)",
                      f"Registro que originou o problema: {item}."
                      f"Informações Técnicas: {str(erro)}")
    self.conectorBD.commit()
    pass