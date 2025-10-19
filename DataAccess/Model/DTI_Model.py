class DTIModel():
    def __init__(self):
        self._interactionId : str
        self._drugId : str
        self._proteinId :str
    
    def _areParametersCorrectedFromText(self, drugId : str, proteinId : str):

        if(proteinId == None or proteinId == ""):
            print("The proteinId is null or empty")
            return False
        
        if(drugId == None or drugId == ""):
            print("The drugId is null or empty")
            return False
        
        return True
    
    def setValuesFromText(self,  drugId : str, proteinId : str):
                
        if(not self._areParametersCorrectedFromText(drugId,proteinId)):
            exit()
        
        self._drugId = drugId.strip()
        self._proteinId= proteinId.strip()
        
    def _areParametersCorrectedFromMongo(self, drugId : str, proteinId : str):

        if(proteinId == None or proteinId == ""):
            print("The proteinId is null or empty")
            return False
        
        if(drugId == None or drugId == ""):
            print("The drugId is null or empty")
            return False
        
        return True
            
    def setValuesFromMongo(self,  drugId : str, proteinId : str):
                
        if(not self._areParametersCorrectedFromMongo(drugId,proteinId)):
            exit()
        
        self._drugId = drugId
        self._proteinId = proteinId
    
    def setIdInteraction(self, id:str):
        self._interactionId = id
            
    def toString(self):
        return "DrugId "+ self._drugId + " ProteinId "+ self._proteinId
        
    def toDict(self):
        return {
            "drugId" : self._drugId,
            "proteinId" : self._proteinId,
        }