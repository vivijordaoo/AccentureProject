class BKP(object):

    def criar_BKP(self, conector_API):
        arquivo_BKP = open(r"Dados.csv", "a+")
        for linha in conector_API:
            arquivo_BKP.write(linha["Country"] + "," + linha["ISO2"] + "\n")
        arquivo_BKP.close()