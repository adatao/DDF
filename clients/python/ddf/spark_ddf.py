
from gateway import start_gateway_server
from ddf import DDF

class DDFManager(object):
    """
    Main entry point for DDF functionality. A SparkDDFManager can be used
    to create DDFs that are implemented for Spark framework.
    """
    
    _gateway = None
    _jvm = None
    
    def __init__(self, jdm):
        """
        create a new SparkDDFManager.
        
        """
        DDFManager._initialized(self)
        self._jdm = jdm
        
    @classmethod
    def _initialized(cls, instance = None):
        if not DDFManager._gateway:
            DDFManager._gateway = start_gateway_server()
            DDFManager._jvm = DDFManager._gateway.jvm

    @classmethod
    def get(cls, engineName):
        DDFManager._initialized()
        return DDFManager(cls._jvm.com.adatao.ddf.DDFManager.get(engineName))
        
    """
    Create a DDF from an sql command.
    """
    def sql2txt(self, command):
        return self._jdm.sql2txt(command)

    def sql2ddf(self, command):
        return DDF(self._jdm.sql2ddf(command))

