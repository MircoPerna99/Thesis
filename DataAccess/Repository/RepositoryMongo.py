from pymongo import MongoClient
from DataAccess.Model.PPI_Model import PPIModel

class RepositoryMongo():
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.database = self.client["Thesis"]
        
    def insertPPI(self, PPI: PPIModel):
        self.collection = self.database["PPI"]
        self.id_collection = "PPI_Collections"
        
        result = self.collection.insert_one(PPI.toDict())
        PPI.setIdInteraction(result.inserted_id)
        
    def close_connection(self):
        self.client.close()