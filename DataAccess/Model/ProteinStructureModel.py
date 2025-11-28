import sys
import os

sys.path.append(os.path.abspath("../"))
class ProteinStuctureModel():
    def __init__(self):
        self._proteinId : str
        self._fastaStructue :str
    
    def __eq__(self, other):
        if not isinstance(other, ProteinStuctureModel):
            return NotImplemented
        return self._proteinId == other._proteinId and self._fastaStructue == other._fastaStructue

    def __hash__(self):
        return hash((self._proteinId, self._fastaStructue))
    
    def _areParametersCorrectedFromXML(self, proteinId  : str, fastaStructue : str):

        if(fastaStructue == None or fastaStructue == ""):
            print("The smileStructue is null or empty")
            return False
        
        if(proteinId  == None or proteinId  == ""):
            print("The proteinId is null or empty")
            return False
        
        return True
    
    def setValuesFromXML(self,  proteinId  : str, fastaStructue : str):
                
        if(not self._areParametersCorrectedFromXML(proteinId, fastaStructue)):
            exit()
        
        self._proteinId = proteinId 
        self._fastaStructue = fastaStructue
    
    def toDict(self):
        return {
            "proteinId" : self._proteinId,
            "smileStructue" : self._fastaStructue,
        }