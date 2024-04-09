import json
import csv

from processamento_dados import Dados


def leitura_json(path_json):
    dados_jason = []
    with open(path_json, 'r') as file:
        dados_json = json.load(file)
    return dados_json  

def leitura_csv(path_csv):    
    dados_csv = []
    with open(path_csv, 'r') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        for row in spamreader:
            #print(row)
            dados_csv.append(row) 
    return dados_csv        

def leitura_dados(path, tipo_arquivo):
    dados = []
    
    if tipo_arquivo == 'csv':
        dados = leitura_csv(path)
    elif tipo_arquivo == 'json':
        dados = leitura_json(path)
    
    return dados

def get_columns(dados):
    return list(dados[-1].keys())

def rename_columns(dados, key_mapping):
    new_dados_csv = []

    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    
    return new_dados_csv

def size_data(dados):
    return len(dados)

def join(dadosA, dadosB):
    combined_List = []
    combined_List.extend(dadosA)
    combined_List.extend(dadosB) 
    return combined_List

def transformando_dados_tabela(dados, nomes_colunas):
    dados_combinados_tabela = [nomes_colunas]

    for row in dados: 
        linha = []
        for coluna in nomes_colunas:
            linha.append(row.get(coluna, 'Indisponível'))
        dados_combinados_tabela.append(linha)
            
    return dados_combinados_tabela

def salvando_dados(dados, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'


#Iniciando a Leitura

dados_json = leitura_dados(path_json, 'json')
nome_colunas_json = get_columns(dados_json)
tamanho_dados_json = size_data(dados_json)

print(f"Nome colunas JSON: {nome_colunas_json}")
print(f"Quantidade de linhas no JSON: {tamanho_dados_json}")
print("\n")

dados_csv = leitura_dados(path_csv, 'csv')
nome_colunas_csv = get_columns(dados_csv)
tamanho_dados_csv = size_data(dados_csv)

print(f"Nome colunas CSV: {nome_colunas_csv}")
print(f"Quantidade de linhas no CSV: {tamanho_dados_csv}")
print("\n")


key_mapping = {'Nome do Item': 'Nome do Produto', 
                'Classificação do Produto' : 'Categoria do Produto',
                'Valor em Reais (R$)' : 'Preço do Produto (R$)',
                'Quantidade em Estoque' : 'Quantidade em Estoque',
                'Nome da Loja' : 'Filial',
                'Data da Venda' : 'Data da Venda'}

# Transformação dos dados

dados_csv = rename_columns(dados_csv, key_mapping)
nome_colunas_csv = get_columns(dados_csv)
print(f"Nome de colunas final: {nome_colunas_csv}")
print("\n")

dados_fusao = join(dados_json, dados_csv)
nome_colunas_fusao = get_columns(dados_fusao)
tamanho_dados_fusao = size_data(dados_fusao)
print(f"Nome das colunas após a fusão: {nome_colunas_fusao}")
print(f"Tamanho dos dados após a fusão: {tamanho_dados_fusao}")
print("\n")

# Salvando dados

dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nome_colunas_fusao)
path_dados_combinados = 'data_processed/dados_combinados.csv'
salvando_dados (dados_fusao_tabela, path_dados_combinados)

print(path_dados_combinados)
print("\n")