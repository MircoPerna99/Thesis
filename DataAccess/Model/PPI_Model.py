import re
class PPIModel():
    def __init__(self):
        self._interactionId : str
        self._proteinAId : str
        self._proteinBId : str
        self._proteinAIdNum : int
        self._proteinBIdNum : int
        self._score: float
        
        self._dirtyDataOnPotreinIdRegex = ["uniprotkb:"]
        self._dirtyDataOnConfidenceScore = "intact-miscore:"
    
    def _areParametersCorrectedFromText(self, proteinAId : str, proteinBId : str, confidenceScore : str):
        if(proteinAId == None or proteinAId == ""):
            print("The proteinAId is null or empty")
            return False
        
        if(proteinBId == None or proteinBId == ""):
            print("The proteinBId is null or empty")
            return False
        
        if(confidenceScore == None or confidenceScore == ""):
            print("The confidenceScore is null or empty")
            return False
        
        return True
    
    def _cleanProteinId(self, proteinId: str):
        pattern = r"^(" + "|".join(self._dirtyDataOnPotreinIdRegex) + ")"
        regex = re.compile(pattern)
        if(regex.match(proteinId)):
            return re.sub(pattern, "", proteinId)
        else:
            print("New prefix find ", proteinId)
            return None

    def _cleanScore(self, confidenceScore: str):            
        return float(confidenceScore.split(self._dirtyDataOnConfidenceScore)[1])
    
    def setValuesFromText(self,  proteinAId : str, proteinBId : str, confidenceScore : str):
                
        if(not self._areParametersCorrectedFromText(proteinAId,proteinBId,confidenceScore)):
            exit()
        
        self._proteinAId = self._cleanProteinId(proteinAId)
        self._proteinBId = self._cleanProteinId(proteinBId)
        self._score = self._cleanScore(confidenceScore)
        
    def _areParametersCorrectedFromMongo(self, proteinAId : str, proteinBId : str, confidenceScore : float):
        if(proteinAId == None or proteinAId == ""):
            print("The proteinAId is null or empty")
            return False
        
        if(proteinBId == None or proteinBId == ""):
            print("The proteinBId is null or empty")
            return False
        
        if(not isinstance(confidenceScore, float)):
            print("The confidenceScore is not a float type"+ str(confidenceScore))
            return False
        
        return True
        
    def setValuesFromMongo(self,  proteinAId : str, proteinBId : str, confidenceScore : float):
                
        if(not self._areParametersCorrectedFromMongo(proteinAId,proteinBId,confidenceScore)):
            exit()
        
        self._proteinAId = proteinAId
        self._proteinBId = proteinBId
        self._score = confidenceScore
        
    def setIdInteraction(self, id:str):
        self._interactionId = id
        
    def toString(self):
        return "ProteinAId "+ self._proteinAId + " ProteinBId "+ self._proteinBId  + " Score "+ str(self._score)
        
    def toDict(self):
        return {
            "proteinAId" : self._proteinAId,
            "proteinBId" : self._proteinBId,
            "score" : self._score
        }