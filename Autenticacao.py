from http.server import BaseHTTPRequestHandler, HTTPServer
import pymongo
import time
import json
import math
import os
from datetime import datetime
from bson.code import Code
import statistics

#VARIÁVEIS GLOBAIS
hostName = "localhost"
serverPort = 3030
maxEmbedding = 0.3
minTotalAceito = 7
limitParaTeste = 0 #pode-se definir um limite para o /teste, ou usar 0 para rodar em todo os registros do banco
sessions = [0]
sessions_para_teste = [0] #usado no /teste, o sistema irá pegar os dados destas sessões e irá comparar com o "sessions" logo acima

class MyServer(BaseHTTPRequestHandler):
    subj_ids_banco = None

    #QUANTIDADE DE COLUNAS NOS DADOS, USADO NOS FOR. CONVERTE OS NUMEROS EM STRING PARA ACESSAR OS CAMPOS NO BANCO DE DADOS E TAMBÉM NOS DADOS QUE VEM DO POSTMAN
    s_column_index = []
    for columnIndex in range(3, 67):
        s_column_index.append(str(columnIndex))
          
    #MÉTODO PADRÃO QUE SEMPRE SERÁ CHAMADO AO EXECUTAR UM "POST" NO POSTMAN
    def do_POST(self):
        #VARIAVEL UTILIZADA PARA CONTROLAR SE HÁ ALGUM PROBLEMA, COMO FALTA DE ALGUM CAMPO OU ATÉ QUANDO O USUÁRIO NÃO FOR AUTENTICADO
        error = ''
            
        #OBTEM OS DADOS VINDO DO BODY DO POSTMAN  
        if self.path != '/calculo_media':
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
                    #INICIA O BANCO DE DADOS E CHAMA O MÉTODO Calcular_embedding PARA VER SE O USUÁRIO PODE SER AUTENTICADO
                    subj_id = str(subj_id)
                    client = pymongo.MongoClient("localhost", 27017)
                    db = client.Autenticacao2
                    authenticated = False
                    totalOk = self.Calcular_embedding(db, subj_id, self.Calcular_avg_dados(arr_dados), maxEmbedding, True)['totalOk']        
                    if totalOk:
                        authenticated = totalOk >= minTotalAceito
                    if authenticated:
                        #SE O USUÁRIO FOR AUTENTICADO, ENTÃO INSERE O REGISTRO NO BANCO, ATRAVÉS DO MÉTODO Cadastrar
                        if not error:
                            for dados in arr_dados:
                                self.Cadastrar(db.Log, subj_id, dados, self.client_address[0])
                    else:
                        error = 'Usuário não autenticado'
                        
                else:
                    error = 'subj_id não informado'
                #MONTA A RESPOSTA, DIZENDO SE O SUBJ_ID ESTÁ AUTENTICADO OU NÃO
                response = '{"success": ' + str(not error) + ', "error": "' + error + '"}'
                
            #CHAMADA consulta DO POSTMAN
            elif self.path == '/consulta':
                #INICIA O BANCO DE DADOS E CHAMA O MÉTODO ObterSubjIdsCandidatos PARA VER SE HÁ SUBJ_IDS PRÓXIMO AOS DADOS INFORMADOS
                client = pymongo.MongoClient("localhost", 27017)
                db = client.Autenticacao2
                subj_id_candidatos = self.ObterSubjIdsCandidatos(db, arr_dados, True)
                if not subj_id_candidatos:
                    error = 'Nenhum subj_id candidato'
                    
                #MONTA A RESPOSTA, DIZENDO SE HÁ ALGUM SUBJ_ID CORRESPONDENTE AOS DADOS
                response = '{"success": ' + str(not error) + ', "subj_ids": ' + str(subj_id_candidatos) + ', "error": "' + error + '"}'
                
            #CHAMADA consulta DO POSTMAN
            elif self.path == '/processar_subj_ids':
                os.system("Processar_SubjId.py 1")
                response = '{"success": True, "error": ""}'
                
            #CHAMADA cadastro DO POSTMAN
            elif self.path == '/cadastro':
                #INICIA O BANCO DE DADOS E CHAMA O MÉTODO ObterSubjIdsCandidatos PARA VER SE HÁ SUBJ_IDS PRÓXIMO AOS DADOS INFORMADOS
                client = pymongo.MongoClient("localhost", 27017)
                db = client.Autenticacao2
                if len(arr_dados) >= 7:
                    for dados in arr_dados:
                        self.Cadastrar(db.Dados, subj_id, dados, None)                        
                    os.system("Processar_SubjId.py 1")
                else:
                    error = 'É necessário ao menos 7 dados'
                    
                #MONTA A RESPOSTA, DIZENDO SE HÁ ALGUM SUBJ_ID CORRESPONDENTE AOS DADOS
                response = '{"success": ' + str(not error) + ', "error": "' + error + '"}'
                
            #CHAMADA cadastro DO POSTMAN
            elif self.path == '/teste':
                client = pymongo.MongoClient("localhost", 27017)
                db = client.Autenticacao2
                teste_automatizado = {}
                totalTesteRR = 0
                countRR = 0
                for session in sessions_para_teste:
                    registros_subj_ids = db.Subj_Ids.find({'session': session})
                    subj_ids_session = next(registros_subj_ids, None)['subj_ids']
                    subj_ids_session.sort() 
            
                    if limitParaTeste > 0:
                        registros = db.Dados.find({'session': session}).limit(limitParaTeste)
                        registros_len = limitParaTeste 
                    else: 
                        registros = db.Dados.find({'session': session})
                        registros_len = db.Dados.count_documents({'session': session})
                        
                    registros_index = 0
                    percent = 0
                    for registro in registros: 
                        registros_index += 1 
                        perc = int(registros_index / registros_len * 100)
                        if perc > percent:
                            percent = perc
                            print('Processando session ' + str(session) + ': ' + str(percent) + '%')
                        dados = registro.copy()
                        _id = str(dados['_id'])
                        subj_id = str(dados['subj_id'])
                        del dados['_id']
                        del dados['DataHora']
                        del dados['session']
                        del dados['subj_id']
                        
                        avg_dados = self.Calcular_avg_dados([dados]) 
                        if not subj_id in teste_automatizado:
                            teste_automatizado[subj_id] = {
                                'Testes realizados': 0,
                                'Falsos negativos': 0,
                                'Falsos positivos': 0,
                                'Falsos positivos (subj_ids)': [],
                            }
                        
                        menor_embedding = 99;
                        menor_embedding_subj_id = 0;
                        for subj_id_teste in subj_ids_session:
                            s_subj_id_teste = str(subj_id_teste)
                            subj_id_key = 'Pegando os dados do ' + subj_id + '|' + _id + ' e tentando autenticar como se fosse o ' + s_subj_id_teste
                            result = self.Calcular_embedding(db, s_subj_id_teste, avg_dados, maxEmbedding, False)
                            totalOk = result['totalOk']
                            #if subj_id == s_subj_id_teste:
                            #    print(result)
                            if totalOk >= 0:
                                if result['menor_embedding'] < menor_embedding:
                                    menor_embedding = result['menor_embedding']
                                    menor_embedding_subj_id = subj_id_teste
                                    
                                teste_automatizado[subj_id]['Testes realizados'] += 1
                                if (totalOk >= minTotalAceito) and (s_subj_id_teste != subj_id):
                                    teste_automatizado[subj_id]['Falsos positivos'] += 1
                                    teste_automatizado[subj_id]['Falsos positivos (subj_ids)'].append(s_subj_id_teste + ', usando dados do _id ' + _id)
                                if (totalOk < minTotalAceito) and (s_subj_id_teste == subj_id):
                                    teste_automatizado[subj_id]['Falsos negativos'] += 1
                        
                        totalTesteRR += 1
                        if str(menor_embedding_subj_id) == subj_id:
                            countRR += 1
                                
                        #subj_id_candidatos = self.ObterSubjIdsCandidatos(db, [dados], False)
                        #teste_automatizado[subj_id_key] |= subj_id_candidatos[0]
                    
                total_de_testes = 0
                total_de_positivos = 0
                total_de_negativos = 0
                for subj_id in teste_automatizado:
                    total_de_testes += teste_automatizado[subj_id]['Testes realizados']
                    total_de_positivos += teste_automatizado[subj_id]['Falsos positivos']
                    total_de_negativos += teste_automatizado[subj_id]['Falsos negativos']                       
                    
                total_autenticado = total_de_testes - total_de_positivos - total_de_negativos
                
                #MONTA A RESPOSTA, DIZENDO SE HÁ ALGUM SUBJ_ID CORRESPONDENTE AOS DADOS
                response = '{"success": ' + str(not error)
                response += ', "falsos positivos": "' + str(total_de_positivos / total_de_testes * 100) + '%"' 
                response += ', "falsos negativos": "' + str(total_de_negativos / total_de_testes * 100) + '%"' 
                response += ', "Verdadeiro positivos": "' + str(100 - (total_de_positivos / total_de_testes * 100)) + '%"'
                response += ', "Verdadeiro Negativo": "' + str(100 - (total_de_negativos / total_de_testes * 100)) + '%"' 
                response += ', "FAR": "' + str(total_de_positivos / total_autenticado * 100) + '%"' 
                response += ', "FRR": "' + str(total_de_negativos / total_autenticado * 100) + '%"' 
                response += ', "RR": "' + str(countRR / totalTesteRR * 100) + '%"' 
                response += ', "resultados": ' + json.dumps(teste_automatizado)
                response += ', "error": "' + error + '"'
                response += '}'
            #CHAMADA cadastro DO POSTMAN
            elif self.path == '/testeEER':
                client = pymongo.MongoClient("localhost", 27017)
                db = client.Autenticacao2
                for min_total_aceito in range(11, 15):
                    teste_automatizado = {}
                    for session in sessions_para_teste:
                        registros_subj_ids = db.Subj_Ids.find({'session': session})
                        subj_ids_session = next(registros_subj_ids, None)['subj_ids']
                        subj_ids_session.sort() 
                
                        if limitParaTeste > 0:
                            registros = db.Dados.find({'session': session}).limit(limitParaTeste)
                            registros_len = limitParaTeste 
                        else: 
                            registros = db.Dados.find({'session': session})
                            registros_len = db.Dados.count_documents({'session': session})
                            
                        registros_index = 0
                        percent = 0
                        for registro in registros: 
                            registros_index += 1 
                            perc = int(registros_index / registros_len * 100)
                            if perc > percent:
                                percent = perc
                                print('Processando session ' + str(session) + ', min_total_aceito ' + str(min_total_aceito) + ': ' + str(percent) + '%')
                            dados = registro.copy()
                            subj_id = str(dados['subj_id'])
                            del dados['_id']
                            del dados['DataHora']
                            del dados['session']
                            del dados['subj_id']
                            
                            avg_dados = self.Calcular_avg_dados([dados]) 
                            if not subj_id in teste_automatizado:
                                teste_automatizado[subj_id] = {
                                    'Testes realizados': 0,
                                    'Falsos negativos': 0,
                                    'Falsos positivos': 0,
                                }
                            
                            for subj_id_teste in subj_ids_session:
                                s_subj_id_teste = str(subj_id_teste)
                                result = self.Calcular_embedding(db, s_subj_id_teste, avg_dados, maxEmbedding, False)
                                totalOk = result['totalOk']
                                if totalOk >= 0:
                                    teste_automatizado[subj_id]['Testes realizados'] += 1
                                    if (totalOk >= min_total_aceito) and (s_subj_id_teste != subj_id):
                                        teste_automatizado[subj_id]['Falsos positivos'] += 1
                                    if (totalOk < min_total_aceito) and (s_subj_id_teste == subj_id):
                                        teste_automatizado[subj_id]['Falsos negativos'] += 1
                        
                    total_de_testes = 0
                    total_de_positivos = 0
                    total_de_negativos = 0
                    for subj_id in teste_automatizado:
                        total_de_testes += teste_automatizado[subj_id]['Testes realizados']
                        total_de_positivos += teste_automatizado[subj_id]['Falsos positivos']
                        total_de_negativos += teste_automatizado[subj_id]['Falsos negativos']                       
                        
                    total_autenticado = total_de_testes - total_de_positivos - total_de_negativos
                    
                    #MONTA A RESPOSTA, DIZENDO SE HÁ ALGUM SUBJ_ID CORRESPONDENTE AOS DADOS
                    if len(response) > 0:
                        response += ','
                    response += '{"min_total_aceito": ' + str(min_total_aceito)
                    response += ', "FAR": "' + str(total_de_positivos / total_autenticado * 100) + '%"' 
                    response += ', "FRR": "' + str(total_de_negativos / total_autenticado * 100) + '%"' 
                    response += '}'
                
                response = '{"results": [' + response + ']}'
                
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
    def ObterSubjIdsCandidatos(self, db, arr_dados, print_info):
        if self.subj_ids_banco is None:
            registros_subj_ids = db.Subj_Ids.find()
            self.subj_ids_banco = []
            for registro in registros_subj_ids:
                self.subj_ids_banco.extend(registro['subj_ids'])
            self.subj_ids_banco = list(set(self.subj_ids_banco))
            self.subj_ids_banco.sort() 
        
        #PERCORRE POR TODOS OS SUBJ_ID DO BANCO E EXECUTA O Calcular_embedding DE UM POR UM, SE O VALOR ESTIVER ABAIXO DO maxEmbedding ENTÃO CONSIDERA ELE COMO UM "subj_id candidato"
        subj_id_candidatos = {}
        index = 0
        for dados in arr_dados:
            if print_info:
                print('')
                print('')
                print('embedding para os dados do índice ' + str(index) + ':')
                print('')
                
            subj_id_candidatos[index] = {}
            for subj_id_banco in self.subj_ids_banco:
                #embedding = self.Calcular_embedding(db, subj_id, self.Calcular_avg_dados(arr_dados))
                totalOk = self.Calcular_embedding(db, subj_id_banco, dados, maxEmbedding, print_info)['totalOk']
                if totalOk >= minTotalAceito:
                    subj_id_candidatos[index] |= {'subj_id '+ subj_id_banco: 'passou em ' + str(totalOk) + ' registros'}
            
            index = index+1
            
        return subj_id_candidatos
        
    #INSERE UM NOVO DADO DO SUBJ_ID NO MONGO
    def Cadastrar(self, db_table, subj_id, dados, client_ip):
        #MONTA O JSON
        dados_json = {        
            "DataHora": datetime.today(),
            "subj_id": int(subj_id),
            "session": 3,
        }
        if client_ip:
            dados_json['client_ip'] = client_ip
        for columnIndex in self.s_column_index:
            dados_json[columnIndex] = dados[columnIndex]
            
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
    def Calcular_embedding(self, db, subj_id, avg_dados, max_embedding, print_info):
        embeddings = []
        embeddingsOk = {}
        totalOk = 0
        total = 0
        index = 0
        menor_embedding = 99;
        for session in sessions:
            registros = db.Dados.find({'session': session, 'subj_id': int(subj_id)}) 
            for registro in registros:  
                dados = registro.copy()
                _id = str(dados['_id'])
                del dados['_id']
                del dados['DataHora']
                del dados['session']
                del dados['subj_id']
                embedding = math.dist(list(dados.values()), list(avg_dados.values()))
                if embedding <= max_embedding:
                    totalOk += 1
                    embeddingsOk[str(index)+'|'+_id] = embedding
                
                total += 1
                index += 1
                embeddings.append(embedding)
                if ((embedding > 0) and (embedding < menor_embedding)):
                    menor_embedding = embedding
            
        if embeddings:
            media_embedding = statistics.mean(embeddings)
            if print_info and totalOk > 0:
                print('subj_id ' + str(subj_id) + ' passou em ' + str(totalOk) + ' de ' + str(total) + ' registros, com as seguintes distâncias: ' + str(embeddingsOk));
        
            return {'totalOk': totalOk, 'media_embedding': media_embedding, 'menor_embedding': menor_embedding}
        else:
            #RETONA -1 SE NÃO EXISTIR NENHUM REGISTRO PARA O SUBJ_ID INFORMADO DENTRO DOS sessions
            return {'totalOk': -1}

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
   
   