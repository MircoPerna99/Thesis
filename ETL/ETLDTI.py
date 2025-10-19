import sys
import os

sys.path.append(os.path.abspath("../"))

from DataAccess.Repository.RepositoryFile import RepositoryFile
from DataAccess.Repository.RepositoryMongo import RepositoryMongo
from DataAccess.Model.DTI_Model import DTIModel
class ETLDTI():
    def __init__(self):
        self._dfDTI = RepositoryFile('/Users/mircoperna/Documents/Universita/Tesi/Code/Thesis/Dates/DTI/all.csv').readFile(",")

    def splitDrugs(self):
        self._dfDTI["Drug IDs"] = self._dfDTI['Drug IDs'].str.split(';')
        self._dfDTI =  self._dfDTI.explode('Drug IDs', ignore_index=True)
        self._dfDTI = self._dfDTI.drop_duplicates(subset=['UniProt ID','Drug IDs'])
    
    def fromDataFrameToModel(self):
                DTIs = []
                for index, row in self._dfDTI.iterrows():
                        newDTI = DTIModel()
                        newDTI.setValuesFromText(row['UniProt ID'], row['Drug IDs'])
                        DTIs.append(newDTI)
                
                return DTIs
            
    def syncFromTextToMongo(self):
                self.splitDrugs()
                DTIs = self.fromDataFrameToModel()
                
                repositoryMongo = RepositoryMongo()

                for dti in DTIs:
                        repositoryMongo.insertDTI(dti)
                        
                repositoryMongo.close_connection()
                

etl = ETLDTI()
etl.syncFromTextToMongo()