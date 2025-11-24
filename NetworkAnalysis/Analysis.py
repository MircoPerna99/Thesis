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
        { "proteinAId": "P04035" },
        { "proteinBId": "P04035" }
      ]
    },
    { "score": { "$gte": 0.5 } }
  ]
}"""


repositoryMongo = RepositoryMongo()
PPIs = repositoryMongo.readPPIs(query)

query = """{ 
  "$and": [
    {
      "$or": [
        { "proteinAId": "P27487" },
    	{ "proteinBId": "P27487" }
      ]
    },
    { "score": { "$gte": 0.5 } }
  ]
}"""

repositoryMongo = RepositoryMongo()
PPIs.extend(repositoryMongo.readPPIs(query))


query = """{ 
  "$and": [
    {
      "$or": [
        { "proteinAId": "P35869" },
        { "proteinBId": "P35869" }
      ]
    },
    { "score": { "$gte": 0.5 } }
  ]
}"""
PPIs.extend(repositoryMongo.readPPIs(query))

query = """{ 
  "$and": [
    {
      "$or": [
        { "proteinAId": "Q92769" },
        { "proteinBId": "Q92769" }
      ]
    },
    { "score": { "$gte": 0.5 } }
  ]
}"""
PPIs.extend(repositoryMongo.readPPIs(query))


query = """{ 
  "$and": [
    {
      "$or": [
        { "proteinAId": "Q14994" },
        { "proteinBId": "Q14994" }
      ]
    },
    { "score": { "$gte": 0.5 } }
  ]
}"""
PPIs.extend(repositoryMongo.readPPIs(query))

DTIs = []
if(True):
  query = '{"drugId":"DB01076"}'
  DTIs = repositoryMongo.readDTIs(query)
else:
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