import re
class PPIModel():
    def __init__(self, proteinAId : str, proteinBId : str, confidenceScore : str):
        self._proteinAId : str
        self._proteinBId : str
        self._score: float
        
        self._dirtyDataOnPotreinIdRegex = ["uniprotkb:", "intact:"]
        self._dirtyDataOnConfidenceScore = "intact-miscore:"
        
        if(not self._areParametersCorrected(proteinAId,proteinBId,confidenceScore)):
            exit()
        
        self._proteinAId = self._cleanProteinId(proteinAId)
        self._proteinBId = self._cleanProteinId(proteinBId)
        self._score = self._cleanScore(confidenceScore)
    
    def _areParametersCorrected(self, proteinAId : str, proteinBId : str,confidenceScore : str):
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
        if(self._dirtyDataOnConfidenceScore in confidenceScore):
            return float(confidenceScore.replace(self._dirtyDataOnConfidenceScore, ""))
        
    def toString(self):
        print(self._proteinAId, self._proteinBId, self._score)