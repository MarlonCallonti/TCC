import pandas as pd
import math
import pymongo
from datetime import datetime   
   
#INICIA O BANCO E OBTEM A DataHora DO ULTIMO CALCULO DE MÉDIAS REALIZADO
client = pymongo.MongoClient("localhost", 27017)
db = client.Autenticacao
registros = db.Medias.find().sort("DataHora", pymongo.DESCENDING)
ultimo_registro = next(registros, None)
ultimo_calculo_dh = None if not ultimo_registro else ultimo_registro['DataHora']

#QUANTIDADE DE COLUNAS NOS DADOS, USADO NOS FOR. CONVERTE OS NUMEROS EM STRING PARA ACESSAR OS CAMPOS NO BANCO DE DADOS E TAMBÉM NOS DADOS QUE VEM DO POSTMAN
s_column_index = []
for columnIndex in range(3, 67):
    s_column_index.append(str(columnIndex))
    
#INICIALIZAÇÃO DAS VARIAVEIS
counts = {}
totalsForMedia = {}
medias = {}

#OBTEM TODOS OS DADOS SALVOS APÓS O ultimo_calculo_dh, E PERCORRE CADA UM DELES NO FOR
if ultimo_calculo_dh:
    dados = db.Dados.find({'DataHora': {'$gt': ultimo_calculo_dh}})
else:
    dados = db.Dados.find()

for row in dados:
    s_subj_id = str(row['subj_id'])
    if (s_subj_id not in counts):
        #INICIA AS VARIAVEIS COM ALGUM VALOR, CASO SEJA A PRIMEIRA VEZ DO FOR QUE ESTÁ PASSANDO POR UM SUBJ_ID
        counts[s_subj_id] = {}
        totalsForMedia[s_subj_id] = {}
        for columnIndex in s_column_index:
            counts[s_subj_id][columnIndex] = 0
            totalsForMedia[s_subj_id][columnIndex] = 0
        
    #FAZ UMA CONTAGEM DE TODOS OS REGISTROS NOVOS QUE TEM DESTE SUBJ_ID (VARIÁVEL counts), E TAMBÉM SOMA TODOS OS VALORES DENTRO DA VARIÁVEL totalsForMedia
    print(totalsForMedia)
    for columnIndex in s_column_index:
        columnValue = row['_'+columnIndex]
        counts[s_subj_id][columnIndex] = counts[s_subj_id][columnIndex] + 1
        totalsForMedia[s_subj_id][columnIndex] = totalsForMedia[s_subj_id][columnIndex] + columnValue
        print(totalsForMedia)
      
#PERCORRE TODOS OS SUBJ_IDS DEFINIDOS NO FOR ANTERIOR E FAZ O CALCULO DA MÉDIA DOS REGISTROS      
for s_subj_id in counts:
    medias[s_subj_id] = {}
    for columnIndex in s_column_index:
        total = totalsForMedia[s_subj_id][columnIndex]
        count = counts[s_subj_id][columnIndex]
        medias[s_subj_id][columnIndex] = total / count #A MÉDIA É FEITO AQUI, PEGANDO A SOMA (total) DE TODOS OS REGISTROS E DIVIDINDO PELO NÚMERO DE REGISTROS (count)
    
#PERCORRE TODOS OS SUBJ_ID CALCULADOS NO ULTIMO FOR
novoRegistroBD = {}
for s_subj_id in medias:
    registros = db.Medias.find({'Medias'+s_subj_id: {'$exists': True}}).sort("DataHora", pymongo.DESCENDING)
    ultimo_registro = next(registros, None)
    #VERIFICA SE JÁ EXISTE ALGUMA MÉDIA CALCULADO PARA ESTE SUBJ_ID DENTRO DO MONGO
    if ultimo_registro:
        for columnIndex in s_column_index:
            #SE JÁ EXISTE UMA MÉDIA ANTERIOR, ENTÃO INSERE A NOVA MÉDIA ATRAVÉS DE UM SISTEMA DE PESO, ONDE A MÉDIA JÁ EXISTENTE TEM PESO 9 E A NOVA MÉDIA TEM PESO 1
            #POR EXEMPLO:
            #  Se a média no banco está em 10, e a média calculada neste arquivo ficou em 20, em vez de simplesmente salvar 15 no banco (que seria um média simples entre 10 e 20), 
            #  com este sistema de peso a nova média ficará em 11, pois 10 * 9 = 90, + 20 = 110, dividido por 10 = 11.
            media_banco = ultimo_registro['Medias'+s_subj_id][columnIndex]
            medias[s_subj_id][columnIndex] = (medias[s_subj_id][columnIndex] + (media_banco * 9)) / 10
            
    novoRegistroBD['Medias'+s_subj_id] = medias[s_subj_id]

#INSERE A NOVA MÉDIA NO MONGO
novoRegistroBD["DataHora"] = datetime.today()
db.Medias.insert_one(novoRegistroBD)

print('Novas médias calculadas!')