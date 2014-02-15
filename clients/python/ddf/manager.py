
from gateway import start_gateway_server

class SparkDDFManager(object):
    """
    Main entry point for DDF functionality. A DDFManager can be used
    to create DDFs.
    """
    
    _gateway = None
    _jvm = None
    
    def __init__(self):
        """
        create a new DDFManager.
        
        """
        DDFManager._initialized(self)
        self._jdc = self._jvm.com.adatao.ddf.spark.SparkDDFManager()
        
    @classmethod
    def _initialized(cls, instance = None):
        if not DDFManager._gateway:
            DDFManager._gateway = start_gateway_server()
            DDFManager._jvm = DDFManager._gateway.jvm
