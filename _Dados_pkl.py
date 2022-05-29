import pandas as pd
import math
from pymongo import MongoClient
from datetime import datetime

#SALVA OS DADOS DO pkl DENTRO DO MONGO

df = pd.read_pickle('encoded_triplet_loss.pkl')
   
client = MongoClient("localhost", 27017)
db = client.Autenticacao2

for row in df.itertuples():
    db.Dados.insert_one ({
        "DataHora": datetime.today(),
        "session": row.session,
        "subj_id": row.subj_id,
        "3": row._3,
        "4": row._4,
        "5": row._5,
        "6": row._6,
        "7": row._7,
        "8": row._8,
        "9": row._9,
        "10": row._10,
        "11": row._11,
        "12": row._12,
        "13": row._13,
        "14": row._14,
        "15": row._15,
        "16": row._16,
        "17": row._17,
        "18": row._18,
        "19": row._19,
        "20": row._20,
        "21": row._21,
        "22": row._22,
        "23": row._23,
        "24": row._24,
        "25": row._25,
        "26": row._26,
        "27": row._27,
        "28": row._28,
        "29": row._29,
        "30": row._30,
        "31": row._31,
        "32": row._32,
        "33": row._33,
        "34": row._34,
        "35": row._35,
        "36": row._36,
        "37": row._37,
        "38": row._38,
        "39": row._39,
        "40": row._40,
        "41": row._41,
        "42": row._42,
        "43": row._43,
        "44": row._44,
        "45": row._45,
        "46": row._46,
        "47": row._47,
        "48": row._48,
        "49": row._49,
        "50": row._50,
        "51": row._51,
        "52": row._52,
        "53": row._53,
        "54": row._54,
        "55": row._55,
        "56": row._56,
        "57": row._57,
        "58": row._58,
        "59": row._59,
        "60": row._60,
        "61": row._61,
        "62": row._62,
        "63": row._63,
        "64": row._64,
        "65": row._65,
        "66": row._66,
    })
 
print('Salvo com sucesso')