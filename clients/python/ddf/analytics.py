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
        
class FiveNumSummary(object):
    def __init__(self, jFiveNumSummary):
        self._jFiveNumSummary = jFiveNumSummary
    
    def getMin(self):
        return self._jFiveNumSummary.getMin()
        
    def getMax(self):
        return self._jFiveNumSummary.getMax()

    def getMedian(self):
        return self._jFiveNumSummary.getMedian()

    def getFirst_quantile(self):
        return self._jFiveNumSummary.getFirst_quantile()

    def getThird_quantile(self):
        return self._jFiveNumSummary.getThird_quantile()
        
"""        
class AggregationResult(object):
    def __init__(self, jAggregationResult):
        self._jAggregationResult = jAggregationResult
        
    
"""        
        
        
        
