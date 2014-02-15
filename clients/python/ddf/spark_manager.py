
from gateway import start_gateway_server

class SparkDDFManager(object):
    """
    Main entry point for DDF functionality. A SparkDDFManager can be used
    to create DDFs that are implemented for Spark framework.
    """
    
    _gateway = None
    _jvm = None
    
    def __init__(self):
        """
        create a new SparkDDFManager.
        
        """
        SparkDDFManager._initialized(self)
        self._jdc = self._jvm.com.adatao.ddf.spark.SparkDDFManager()
        
    @classmethod
    def _initialized(cls, instance = None):
        if not SparkDDFManager._gateway:
            SparkDDFManager._gateway = start_gateway_server()
            SparkDDFManager._jvm = SparkDDFManager._gateway.jvm
