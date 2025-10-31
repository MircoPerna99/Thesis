from pymongo import MongoClient
from DataAccess.Model.PPI_Model import PPIModel
from DataAccess.Model.DTI_Model import DTIModel
import json
from configuration import Configuration

class RepositoryMongo():
    def __init__(self):
        self.config = Configuration()
        self.client = MongoClient(self.config['connection_string_mongo'])
        self.database = self.client["Thesis"]
        
    def insertPPI(self, PPI: PPIModel):
        self.collection = self.database["PPI"]
        self.id_collection = "PPI_Collections"
        
        result = self.collection.insert_one(PPI.toDict())
        PPI.setIdInteraction(result.inserted_id)
       
    def readPPIs(self, query = None):
        self.collection = self.database["PPI"]
        
        if(query == None or query == ""):
            PPIsMongo = self.collection.find()
        else:
            queryToApply = json.loads(query)
            PPIsMongo = self.collection.find(queryToApply).limit(20)
        
        PPIs = []
        for ppi in PPIsMongo:
            ppiToInsert = PPIModel()
            ppiToInsert.setValuesFromMongo(proteinAId=ppi["proteinAId"], proteinBId=ppi["proteinBId"], confidenceScore=ppi["score"])
            PPIs.append(ppiToInsert)
        
        return PPIs

    def readPPI(self, query):
        if(self.isQueryNull(query)):
            exit()
        
        self.collection = self.database["PPI"]
        queryToApply = json.loads(query)
        ppi = self.collection.find_one(queryToApply)
        if(ppi != None):
            ppiToReturn = PPIModel()
            ppiToReturn.setValuesFromMongo(proteinAId=ppi["proteinAId"], proteinBId=ppi["proteinBId"], confidenceScore=ppi["score"])
        else:
            ppiToReturn = None
            
        return ppiToReturn
         
    def insertDTI(self, DTI: DTIModel):
        self.collection = self.database["DTI"]
                
        result = self.collection.insert_one(DTI.toDict())
        DTI.setIdInteraction(result.inserted_id)
    
    def readDTIs(self, query : str = None):
        self.collection = self.database["DTI"]
        
        if(query == None or query == ""):
            DTIsMongo = self.collection.find()
        else:
            queryToApply = json.loads(query)
            DTIsMongo = self.collection.find(queryToApply).limit(10)
        
        DTIs = []
        for dti in DTIsMongo:
            dtiToInsert = DTIModel()
            dtiToInsert.setValuesFromMongo(drugId=dti["drugId"], proteinId=dti["proteinId"])
            DTIs.append(dtiToInsert)
        return DTIs
        
        
    def close_connection(self):
        self.client.close()
        
    def isQueryNull(self, query):
        if(query == None or query == ""):
            print("Query cannot be null")
            return True
        
        return False
    
    
    