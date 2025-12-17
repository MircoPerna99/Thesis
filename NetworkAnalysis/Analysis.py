import sys
import os

sys.path.append(os.path.abspath("../"))
from DataAccess.Repository.RepositoryMongo import RepositoryMongo
from Services.Graphs.Graph import Graph
import pandas as pd

query = """{ 
  "$and": [
    {
      "$or": [
        { "proteinAId": "P49815" },
        { "proteinBId": "P49815" }
      ]
    },
    { "score": { "$gte": 0.5 } }
  ]
}"""


repositoryMongo = RepositoryMongo()
PPIs = repositoryMongo.readPPIs(query)

PPIsToAdd = []
for ppi in PPIs:
  query = f"""{{ 
            "$and": [
              {{
                "$or": [
                  {{ "proteinAId": "{ppi._proteinAId}" }},
                  {{ "proteinBId": "{ppi._proteinAId}" }},
                  {{ "proteinAId": "{ppi._proteinBId}" }},
                  {{ "proteinBId": "{ppi._proteinBId}" }}
                ]
              }},
              {{ "score": {{ "$gte": 0.5 }} }}
            ]
          }}"""
  PPIsToAdd.extend(repositoryMongo.readPPIs(query))

PPIs.extend(PPIsToAdd)

DTIs = []

for ppi in PPIs:
      query = '{"proteinId": "'+ppi._proteinAId+'"}'
      DTIsToAdd = repositoryMongo.readDTIs(query)
      if(DTIsToAdd != None or len(DTIsToAdd) != 0):
          DTIs.extend(DTIsToAdd)
          
      query = '{ "proteinId": "'+ppi._proteinBId+'"}'
      DTIsToAdd = repositoryMongo.readDTIs(query)
      if(DTIsToAdd != None or len(DTIsToAdd) != 0):
          DTIs.extend(DTIsToAdd)

repositoryMongo.close_connection()

dfPPI = pd.DataFrame().from_records(ppi.toDict() for ppi in PPIs)
dfDTI = pd.DataFrame().from_records(dti.toDict() for dti in DTIs)
Graph().printBiologicalNetwork(dfDTI,dfPPI)
#Graph().printDataFrame(dfPPI,"proteinAId", "proteinBId")