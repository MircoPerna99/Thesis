import sys
import os

sys.path.append(os.path.abspath("../"))
from DataAccess.Repository.RepositoryMongo import RepositoryMongo
from DataAccess.Model.DTI_Model import DTIModel
import pandas as pd
from Services.Graphs.Graph import Graph

class ALSDataset():
    def __init__(self):
        pass
    
    def _takeDTI(self, query = None):
        repositoryMongo = RepositoryMongo()
        DTIs = repositoryMongo.readDTIs(query)
        repositoryMongo.close_connection()
        return DTIs
    
    def _takePPI(self):
        repositoryMongo = RepositoryMongo()
        PPIs = repositoryMongo.readPPIs()
        repositoryMongo.close_connection()
        return PPIs

# dataset = ALSDataset()
# dtis = dataset._takeDTI('{ "proteinId": "P35228" }')
# for dti in dtis:
#     print(dti.toString())
    
PPIs = []
repositoryMongo = RepositoryMongo()


proteins = ["O54824", "Q00403", "Q12933", "Q09472", "Q92793", "Q9WU01"]

# for protein in proteins:
#     query = '{ "proteinBId": "'+protein+'"}'
#     PPIsToAdd = repositoryMongo.readPPIs(query)
#     if(PPIsToAdd != None or len(PPIsToAdd) != 0):
#         PPIs.extend(PPIsToAdd)
#         for ppi in PPIsToAdd:
#             query = '{ "proteinBId": "'+ppi._proteinAId+'"}'
#             PPIsToAddA = repositoryMongo.readPPIs(query)
#             if(PPIsToAddA != None or len(PPIsToAddA) != 0):
#                 PPIs.extend(PPIsToAddA)
    
# print(len(PPIs))

PPIs = repositoryMongo.readPPIs()
print(len(PPIs))

dfPPI = pd.DataFrame().from_records(ppi.toDict() for ppi in PPIs)
# Graph().printDataFrame(dfPPI, "proteinAId", "proteinBId")

DTIs = []
for ppi in PPIs:
    query = '{ "proteinId": "'+ppi._proteinAId+'"}'
    DTIsToAdd = repositoryMongo.readDTIs(query)
    if(DTIsToAdd != None or len(DTIsToAdd) != 0):
        DTIs.extend(DTIsToAdd)
    
    query = '{ "proteinId": "'+ppi._proteinBId+'"}'
    DTIsToAdd = repositoryMongo.readDTIs(query)
    if(DTIsToAdd != None or len(DTIsToAdd) != 0):
        DTIs.extend(DTIsToAdd)
    
     
print(len(DTIs))
repositoryMongo.close_connection()
    
dfDTI = pd.DataFrame().from_records(dti.toDict() for dti in DTIs)
# Graph().printHeterogenousGraph(dfDTI,"drugId", "proteinId")

Graph().printBiologicalNetwork(dfDTI,dfPPI)

