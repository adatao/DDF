from analytics import Summary
from analytics import FiveNumSummary

class DDF(object):
    def __init__(self, jddf):
        self._jddf = jddf
        self.Views = ViewsFacade(self._jddf.getViews())
    
    def getSummary(self):
        jsummary = self._jddf.getSummary()
        psummary = []
        for s in jsummary:
            psummary.append(Summary(s))
        return psummary

    def getFiveNumSummary(self):
        jFiveSummary = self._jddf.getFiveNumSummary()
        pFiveSummary = []
        for s in jFiveSummary:
            pFiveSummary.append(FiveNumSummary(s))
        return pFiveSummary

    def getColumnNames(self):
        return self._jddf.getColumnNames()
        
    def getNumRows(self):
        return self._jddf.getNumRows()
        
    def getNumColumns(self):
        return self._jddf.getNumColumns()
        
    def aggregate(self, fields):
        return self._jddf.aggregate(fields)
        

class ViewsFacade(object):
    def __init__(self, jViewsFacade):
        self._jViewsFacade = jViewsFacade

    def firstNRows(self, numRows):
        return DDF(self._jViewsFacade.firstNRows(numRows))


