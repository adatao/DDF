
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
        self._jsdm = self._jvm.com.adatao.spark.ddf.SparkDDFManager()
        
    @classmethod
    def _initialized(cls, instance = None):
        if not SparkDDFManager._gateway:
            SparkDDFManager._gateway = start_gateway_server()
            SparkDDFManager._jvm = SparkDDFManager._gateway.jvm
    """
    Create a DDF from an sql command.
    """
    def cmd2txt(self, command):
        return self._jsdm.cmd2txt(command)

    def cmd2ddf(self, command):
        return self._jsdm.cmd2ddf(command)

