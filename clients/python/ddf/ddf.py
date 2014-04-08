from analytics import Summary
from analytics import FiveNumSummary

class DDF(object):
    def __init__(self, jddf):
        self._jddf = jddf
    
    def getSummary(self):
        jsummary = self._jddf.getSummary()
        psummary = []
        for s in jsummary:
            psummary.append(Summary(s))
        return psummary

    def getFiveNumSummary(self):
        return FiveNumSummary(self._jddf.getFiveNumSummary()
        
    def getColumnNames(self):
        return self._jddf.getColumnNames()
        
    def getNumRows(self):
        return self._jddf.getNumRows()
        
    def getNumColumns(self):
        return self._jddf.getNumColumns()
        
    
