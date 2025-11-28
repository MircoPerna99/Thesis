import sys
import os

sys.path.append(os.path.abspath("../"))

from DataAccess.Repository.RepositoryXML import RepositoryXML
from DataAccess.Repository.RepositoryMongo import RepositoryMongo
from DataAccess.Repository.RepositoryMySQL import RepositoryMySql
from DataAccess.Model.DTI_Model import DTIModel

class ETLStructures():
    def __init__(self):
        self.repositoryXML = RepositoryXML()
            
    def syncFromTextToMongo(self):
        self.repositoryXML.takeStructes()
                
        repositoryMongo = RepositoryMongo()
        for structure in self.repositoryXML.structureDrugs:
            print(structure._drugId)
            repositoryMongo.insertStructureDrug(structure)
           
        for structure in self.repositoryXML.structureProteins:
            repositoryMongo.insertStructureProtein(structure)             
        repositoryMongo.close_connection()
    
    def syncFromMongoToMySql(self):                
        repositoryMongo = RepositoryMongo()

        DTIsToAdd = repositoryMongo.readDTIs()      
        repositoryMongo.close_connection()
        
        repositoryMySql = RepositoryMySql()
        repositoryMySql.addDTIs(DTIsToAdd)

etl = ETLStructures()
etl.syncFromTextToMongo()