import pandas as pd
import math
import pymongo
from datetime import datetime   
   
#INICIA O BANCO E OBTEM A DataHora DO ULTIMO CALCULO DE MÉDIAS REALIZADO
client = pymongo.MongoClient("localhost", 27017)
db = client.Autenticacao2
registros_session = db.Dados.find().distinct('session')
db.Subj_Ids.delete_many({})
for session in registros_session:
    registros_subj_id = db.Dados.find({'session': session}).distinct('subj_id')
    db.Subj_Ids.insert_one({'session': session, 'subj_ids': registros_subj_id})

print('Subj ids processados!')