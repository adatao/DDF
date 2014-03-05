from analytics import Summary

class DDF(object):
    def __init__(self, jddf):
        self._jddf = jddf
    
    def getSummary(self):
        return Summary(self._jddf.getSummary())

