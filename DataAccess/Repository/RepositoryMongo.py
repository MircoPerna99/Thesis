from pymongo import MongoClient
from DataAccess.Model.PPI_Model import PPIModel
from DataAccess.Model.DTI_Model import DTIModel

class RepositoryMongo():
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.database = self.client["Thesis"]
        
    def insertPPI(self, PPI: PPIModel):
        self.collection = self.database["PPI"]
        self.id_collection = "PPI_Collections"
        
        result = self.collection.insert_one(PPI.toDict())
        PPI.setIdInteraction(result.inserted_id)
       
    def readPPIs(self):
        self.collection = self.database["PPI"]
        
        PPIsMongo = self.collection.find()
        
        PPIs = []
        for ppi in PPIsMongo:
            ppiToInsert = PPIModel()
            ppiToInsert.setValuesFromMongo(proteinAId=ppi["proteinAId"], proteinBId=ppi["proteinBId"], confidenceScore=ppi["score"])
            PPIs.append(ppiToInsert)
        
        return PPIs
         
    def insertDTI(self, DTI: DTIModel):
        self.collection = self.database["DTI"]
                
        result = self.collection.insert_one(DTI.toDict())
        DTI.setIdInteraction(result.inserted_id)
    
    def readDTIs(self):
        self.collection = self.database["DTI"]
        
        DTIsMongo = self.collection.find()
        
        DTIs = []
        for dti in DTIsMongo:
            dtiToInsert = DTIModel()
            dtiToInsert.setValuesFromMongo(proteinAId=dti["drugId"], proteinBId=dti["proteinId"])
            DTIs.append(dtiToInsert)
        
        
    def close_connection(self):
        self.client.close()
    
    
    