import sys
import os

sys.path.append(os.path.abspath("../"))
from Services.configuration import Configuration
import mysql.connector # type: ignore

class RepositoryMySql():
    def __init__(self):
        self.config = Configuration()
          
    def _startConnetion(self):
        return mysql.connector.connect(
                            host=self.config["sqlHost"],
                            user=self.config["sqlUser"],
                            password=self.config["sqlPassword"],
                            database=self.config["sqlDatabase"]
                            )
    
    def addDTIs(self,  DTIs:list):
        
        connection = self._startConnetion()
        cursor = connection.cursor()
        for dti in DTIs:
            sql = "INSERT INTO tesi.DTI (DrugId, ProteinId) VALUES(%s, %s);"
            val = (dti._drugId, dti._proteinId)
            
            cursor.execute(sql, val)

        connection.commit()
        
    def addPPIs(self,  PPIs:list):
        
        connection = self._startConnetion()
        cursor = connection.cursor()
        for ppi in PPIs:
            sql = "INSERT INTO tesi.PPI (ProteinAId, ProteinBId, Score) VALUES(%s, %s, %s);"
            val = (ppi._proteinAId, ppi._proteinBId, ppi._score)
            
            cursor.execute(sql, val)

        connection.commit()