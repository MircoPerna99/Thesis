import sys
import os

sys.path.append(os.path.abspath("../"))
class DrugStuctureModel():
    def __init__(self):
        self._drugId : str
        self._smileStructue :str
    
    def __eq__(self, other):
        if not isinstance(other, DrugStuctureModel):
            return NotImplemented
        return self._drugId == other._drugId and self._smileStructue == other._smileStructue

    def __hash__(self):
        return hash((self._drugId, self._smileStructue))
    
    def _areParametersCorrectedFromXML(self, drugId : str, smileStructue : str):

        if(smileStructue == None or smileStructue == ""):
            print("The smileStructue is null or empty")
            return False
        
        if(drugId == None or drugId == ""):
            print("The drugId is null or empty")
            return False
        
        return True
    
    def setValuesFromXML(self,  drugId : str, smileStructue : str):
                
        if(not self._areParametersCorrectedFromXML(drugId,smileStructue)):
            exit()
        
        self._drugId = drugId
        self._smileStructue = smileStructue
    
    def toDict(self):
        return {
            "drugId" : self._drugId,
            "smileStructue" : self._smileStructue,
        }