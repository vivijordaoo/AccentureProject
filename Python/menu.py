

def recebeOpcaoUsuario():
    opcao = 0

    print("Digite a opção que deseja executar:\n"
          "1 - Carrega dados do IBGE\n"
          "2 - Limpa Base Azure\n"
          "3 - Imprimir Todos Distritos\n"
          "4 - Cria estrutura do DB\n"
          "5 - Sair do Programa\n")

    while opcao < 1 or opcao > 5:
        opcao = int(input("Digite uma opção válida (1 - 5): "))
        if opcao < 1 or opcao > 5:
            print("Opção inválida. Digite novamente")

    return opcao

if __name__ == '__main__':
    opcao = recebeOpcaoUsuario()

    while opcao >= 1 and opcao <= 4:
        if opcao == 1: #Carregar Dados do IBGE
            print("Opção 1 - Carrega dados do IBGE\n")            
            carregaIBGE2()

        elif opcao == 2: #Limpa Base
            print("Opção2 - Limpa Base Azure")
            limpaTudo()

        elif opcao == 3: #Imprimir Todos Distritos
            print("Opção3 - Imprimir Todos Distritos")
            mostraDistrito()

        elif opcao == 4: #Cria DB
            print("Opção4 - Cria estrutura DB")
            criaTable()

        else: #sair do programa
            print("Você saiu do programa. Obrigado por usar...\n")
            opcao = 5

        if opcao != 5:
            opcao = recebeOpcaoUsuario()

