
class DDFContext(object):
    """
    Main entry point for DDF functionality. A DDFContext can be used
    to create DDFs.
    """
    
    _gateway = None
    _jvm = None
    
    def __init__(self, master):
        """
        create a new DDFContext.
        
        """
        DDFContext._initialized(self)
        self._jdc = self._jvm.com.adatao.ddf.DDFContextManager.getDDFContext(master)
        
    @classmethod
    def _initialized(cls, instance = None):
        if not DDFContext._gateway:
            DDFContext._gateway = start_gateway_server()
            DDFContext._jvm = DDFContext._gateway.jvm
