from analytics import Summary

class DDF(object):
    def __init__(self, jddf):
        self._jddf = jddf
    
    def getSummary(self):
        jsummary = self._jddf.getSummary()
        psummary = []
        for s in jsummary:
            psummary.append(Summary(s))
        return psummary

