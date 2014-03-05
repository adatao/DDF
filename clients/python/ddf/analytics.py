class Summary(object):
    def __init__(self, jsummary):
        self._jsummary = jsummary
    def min(self):
        return self._jsummary.min()
            
    def max(self):
        return self._jsummary.max()
    
    def variance(self):
        return self._jsummary.variance()
        
    def mean(self):
        return self._jsummary.mean()
