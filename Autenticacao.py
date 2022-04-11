from http.server import BaseHTTPRequestHandler, HTTPServer
import pymongo
import time
import json
import math
import os
from datetime import datetime
from bson.code import Code

#VARIÁVEIS GLOBAIS
hostName = "localhost"
serverPort = 3030
maxEmbedding = 0.3

class MyServer(BaseHTTPRequestHandler):
    #QUANTIDADE DE COLUNAS NOS DADOS, USADO NOS FOR. CONVERTE OS NUMEROS EM STRING PARA ACESSAR OS CAMPOS NO BANCO DE DADOS E TAMBÉM NOS DADOS QUE VEM DO POSTMAN
    s_column_index = []
    for columnIndex in range(3, 67):
        s_column_index.append(str(columnIndex))
          
    #MÉTODO PADRÃO QUE SEMPRE SERÁ CHAMADO AO EXECUTAR UM "POST" NO POSTMAN
    def do_POST(self):
        #VARIAVEL UTILIZADA PARA CONTROLAR SE HÁ ALGUM PROBLEMA, COMO FALTA DE ALGUM CAMPO OU ATÉ QUANDO O USUÁRIO NÃO FOR AUTENTICADO
        error = ''
            
        #OBTEM OS DADOS VINDO DO BODY DO POSTMAN   
        content_length = int(self.headers['Content-Length']) 
        post_data = json.loads(self.rfile.read(content_length))
        arr_dados = post_data.get('dados')
        
        #PERCORRE TODOS OS DADOS PARA VER SE HÁ ALGUM CAMPO FALTANDO
        index = 0
        while (index < len(arr_dados)) and (not error):
            for columnIndex in self.s_column_index:
                if not arr_dados[index][columnIndex]:
                    error = 'campo ' + columnIndex + ' não informado' #SE TIVER UM CAMPO FALTANDO, ELE SAI DO FOR E TERMINA O WHILE
                    break
            index = index+1
           
        response = ''
        if not error:   
            #CHAMADA auth DO POSTMAN
            if self.path == '/auth':
                subj_id = post_data.get('subj_id')
                if subj_id:
                    #INICIA O BANCO DE DADOS E CHAMA O MÉTODO Comparar PARA VER SE O USUÁRIO PODE SER AUTENTICADO
                    subj_id = str(subj_id)
                    client = pymongo.MongoClient("localhost", 27017)
                    db = client.Autenticacao
                    code = self.Comparar(db, subj_id, arr_dados)
                    if code == -1:
                        error = 'Usuário não autenticado'
                    else:
                        #SE O USUÁRIO FOR AUTENTICADO, ENTÃO INSERE O REGISTRO NO BANCO, ATRAVÉS DO MÉTODO Cadastrar
                        if not error:
                            for dados in arr_dados:
                                self.Cadastrar(db.Log, subj_id, dados, self.client_address[0])
                        
                        #EXECUTA O Calculo_media SE FOR UM NOVO SUBJ_ID
                        if code == 0:
                            os.system("Calculo_media.py 1")
                else:
                    error = 'subj_id não informado'
                #MONTA A RESPOSTA, DIZENDO SE O SUBJ_ID ESTÁ AUTENTICADO OU NÃO
                response = '{"success": ' + str(not error) + ', "error": "' + error + '"}'
                
            #CHAMADA consulta DO POSTMAN
            elif self.path == '/consulta':
                #INICIA O BANCO DE DADOS E CHAMA O MÉTODO ObterSubjIdsCandidatos PARA VER SE HÁ SUBJ_IDS PRÓXIMO AOS DADOS INFORMADOS
                client = pymongo.MongoClient("localhost", 27017)
                db = client.Autenticacao
                subj_id_candidatos = self.ObterSubjIdsCandidatos(db, arr_dados)
                if not subj_id_candidatos:
                    error = 'Nenhum subj_id candidato'
                    
                #MONTA A RESPOSTA, DIZENDO SE HÁ ALGUM SUBJ_ID CORRESPONDENTE AOS DADOS
                response = '{"success": ' + str(not error) + ', "subj_ids": ' + str(subj_id_candidatos) + ', "error": "' + error + '"}'
                
            #CHAMADA cadastro DO POSTMAN
            elif self.path == '/cadastro':
                #INICIA O BANCO DE DADOS E CHAMA O MÉTODO ObterSubjIdsCandidatos PARA VER SE HÁ SUBJ_IDS PRÓXIMO AOS DADOS INFORMADOS
                client = pymongo.MongoClient("localhost", 27017)
                db = client.Autenticacao
                for dados in arr_dados:
                    self.Cadastrar(db.Dados, subj_id, dados, None)
                    
                #MONTA A RESPOSTA, DIZENDO SE HÁ ALGUM SUBJ_ID CORRESPONDENTE AOS DADOS
                response = '{"success": ' + str(not error) + ', "error": "' + error + '"}'
                
            #CHAMADA cadastro DO POSTMAN
            elif self.path == '/teste':
                #INICIA O BANCO DE DADOS E CHAMA O MÉTODO ObterSubjIdsCandidatos PARA VER SE HÁ SUBJ_IDS PRÓXIMO AOS DADOS INFORMADOS
                client = pymongo.MongoClient("localhost", 27017)
                db = client.Autenticacao
                totalNaoEncontrados = 0
                minEmbedding = 999
                maxEmbedding = 0
                totalEmbeddings = 0
                count = 0
                totalID=0
                for subj_id in range(1, 223):
                    

                        
                        
                    if response:
                        response = response + ','
                        
                    s_subj_id = str(subj_id)
                    registros = db.Dados.find({'subj_id': subj_id}).sort("DataHora", pymongo.ASCENDING)
                    ultimo_registro = next(registros, None)
                    if ultimo_registro:
                        count = count + 1
                        dados_banco = {}
                        for columnIndex in self.s_column_index:
                            dados_banco[columnIndex] = ultimo_registro['_'+columnIndex]
                            
                        subj_id_candidatos = self.ObterSubjIdsCandidatos(db, [dados_banco])
                        if subj_id_candidatos:
                            if s_subj_id in subj_id_candidatos:
                                subj_id_embedding = subj_id_candidatos[s_subj_id]
                                if subj_id_embedding < minEmbedding:
                                    minEmbedding = subj_id_embedding
                                if subj_id_embedding > maxEmbedding:
                                    maxEmbedding = subj_id_embedding
                                totalEmbeddings = totalEmbeddings + subj_id_embedding  
                            else:
                                totalNaoEncontrados = totalNaoEncontrados + 1 
                        else:
                            totalNaoEncontrados = totalNaoEncontrados + 1
                                
                        response = response + '{"subj_id": ' + s_subj_id + ', "subj_ids_candidatos": ' + str(subj_id_candidatos) + '}'
                        totalID = totalID + 1
                    else:
                        response = response + '{"subj_id": ' + s_subj_id + ', "error": "Nenhuma informação no banco de dados"}'
                        
                #MONTA A RESPOSTA, DIZENDO SE HÁ ALGUM SUBJ_ID CORRESPONDENTE AOS DADOS
                response = '{"totalNaoEncontrados": ' +str(totalNaoEncontrados) +', "Taxa Positivo": '+ str((totalID-totalNaoEncontrados)/totalID) +', "Taxa negativo": '+ str(totalNaoEncontrados/totalID)+', "minEmbedding": ' + str(minEmbedding) + ', "mediaEmbedding": ' + str(totalEmbeddings / count) + ', "maxEmbedding": ' + str(maxEmbedding) + ', "responses": [' + response + '], "error": "' + error + ', "Total ID": "' + str(totalID) +'  "}'
                
            #SE A PAGINA NAO FOR NEM auth NEM subj_id
            else:
                error = 'Página inexistente'
                response = '{"success": False, "error": "' + error + '"}'
        else:
            response = '{"success": False, "error": "' + error + '"}'
               
        #MONTA O RETORNO, UTILIZANDO A MENSAGEM ATRIBUÍDA NOS CÓDIGOS ACIMA
        self.send_response(401 if error else 200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(bytes(response, "utf-8"))
            
    #OBTEM TODOS OS SUBJ_IDS QUE POSSUEM DADOS SEMELHANTE AOS DADOS ENVIADOS NO POSTMAN
    def ObterSubjIdsCandidatos(self, db, arr_dados):
        #CHAMADA AO MONGO PARA OBTER TODOS OS SUBJ_IDS QUE EXISTEM DENTRO DO BANCO
        fields_names = db.Medias.aggregate([
          {"$project":{"arrayofkeyvalue":{"$objectToArray":"$$ROOT"}}},
          {"$unwind":"$arrayofkeyvalue"},
          {"$group":{"_id":None,"allkeys":{"$addToSet":"$arrayofkeyvalue.k"}}}
        ])
        campos_banco = next(fields_names, None)['allkeys']
        
        #PERCORRE POR TODOS OS SUBJ_ID DO BANCO E EXECUTA O Calcular_embedding DE UM POR UM, SE O VALOR ESTIVER ABAIXO DO maxEmbedding ENTÃO CONSIDERA ELE COMO UM "subj_id candidato"
        subj_id_candidatos = {}
        for campo_banco in campos_banco:
            if campo_banco[:6] == 'Medias':
                subj_id = campo_banco[6:] #REMOVE O "Medias" DO NOME DO CAMPO, PARA FICAR SOMENTE O NUMERO DO SUBJ_ID 
                embedding = self.Calcular_embedding(db, subj_id, self.Calcular_avg_dados(arr_dados))
                if embedding < maxEmbedding:
                    subj_id_candidatos[subj_id] = embedding
        
        return subj_id_candidatos
        
    #INSERE UM NOVO DADO DO SUBJ_ID NO MONGO
    def Cadastrar(self, db_table, subj_id, dados, client_ip):
        #MONTA O JSON
        dados_json = {        
            "DataHora": datetime.today(),
            "subj_id": int(subj_id),
        }
        if client_ip:
            dados_json['client_ip'] = client_ip
        for columnIndex in self.s_column_index:
            dados_json['_'+columnIndex] = dados[columnIndex]
            
        #INSERE O JSON NO MONGO
        db_table.insert_one(dados_json)
    
    #FAZ UMA MEDIA DOS DADOS ENVIADO NO POSTMAN
    def Calcular_avg_dados(self, arr_dados):
        avg_dados = {}
        for columnIndex in self.s_column_index:
            avg_dados[columnIndex] = 0
               
        count_dados = len(arr_dados)
        for dados in arr_dados:
            for columnIndex in self.s_column_index:
                avg_dados[columnIndex] = avg_dados[columnIndex] + (dados[columnIndex] / count_dados)
                
        return avg_dados
        
    #CALCULA O EMBEDDING (DISTANCIA) DOS DADOS DO SUBJ_ID. 
    def Calcular_embedding(self, db, subj_id, avg_dados):
        #OBTEM A ULTIMA MEDIA DO SUBJ_ID INFORMADO NO PARAMETRO
        registros = db.Medias.find({'Medias'+subj_id: {'$exists': True}}).sort("DataHora", pymongo.DESCENDING)
        ultimo_registro = next(registros, None)
        embedding = None
        if ultimo_registro:
            #FAZ O CALCULO DO EMBEDDING, USANDO A MEDIA QUE EXISTE NO BANCO E A MEDIA DOS DADOS INFORMADO NO POSTMAN  
            medias = ultimo_registro['Medias'+subj_id]               
            embedding = math.dist(list(medias.values()), list(avg_dados.values()))
            print(subj_id + ' = ' + str(embedding))
            
        return embedding
    
    #COMPARA OS DADOS DO POSTMAN COM O QUE EXISTE NO BANCO, SE O Calcular_embedding RETORNAR UM VALOR DENTRO DO ACEITÁVEL, O USUÁRIO É CONSIDERADO COMO AUTENTICADO
    #ESTE MÉTODO RETORNA 3 CÓDIGOS POSSÍVEIS:
    #   -1: SUBJ_ID NÃO AUTENTICADO
    #    0: SUBJ_ID NÃO EXISTE NO BANCO DE DADOS
    #    1: SUBJ_ID AUTENTICADO
    def Comparar(self, db, subj_id, arr_dados):
        embedding = self.Calcular_embedding(db, subj_id, self.Calcular_avg_dados(arr_dados))
        
        #CONVERSAR COM O ORIENTADOR SOBRE ESSE CALCULO DO math.sqrt(len(arr_dados))}
        #max_embedding = maxEmbedding / math.sqrt(len(arr_dados)) #Calculo de aproximar embedding. Quanto mais dados vier, mais próximo de 0
        #ESSE CALCULO FAZ COM QUE O max_embedding SEJA "EXPREMIDO" EM DIREÇÃO AO 0, POR EXEMPLO, SE MANDAR 9 DADOS NO POSTMAN:
        #   max_embedding = maxEmbedding / math.sqrt(len(arr_dados))
        #   max_embedding = 0.4 / math.sqrt(9)
        #   max_embedding = 0.4 / 3
        #   max_embedding = 0.1333333
        #USANDO O 0.133333, AS CHANCES DE OCORRER UM FALSO POSITIVO SAO MENORES, AFINAL QUANTO MAIS DADOS TEMOS PARA COMPARAR MAIS SEGURO O CALCULO IRÁ FICAR
        #POR EXEMPLO, USANDO O SUBJ_ID 201 NOS TESTES, OS CALCULOS DO EMBEDDING FICARAM ASSIM: 
        #   MANDANDO APENAS 1 REGISTRO, O EMBEDDING RETORNOU 0.2617892171845776
        #   MANDANDO APENAS 3 REGISTROS, O EMBEDDING RETORNOU 0.1889132027110581
        #   MANDANDO APENAS 9 REGISTROS, O EMBEDDING RETORNOU 0.11981542084393451
        #POR ISSO, QUANTO MAIS REGISTROS, MAIS PRÓXIMO A 0 O EMBEDDING TENDE A FICAR
        
        if embedding:
            if embedding > maxEmbedding:
                return -1
            else:
                return 1
        else:
            return 0

#MÉTODO MAIN PARA INICIAR O SERVIDOR
if __name__ == "__main__":        
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
   
   