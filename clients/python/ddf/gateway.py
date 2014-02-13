import os
import sys
import signal

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from subprocess import Popen, PIPE
from threading import Thread

DDF_HOME = os.environ["DDF_HOME"]

def preexec_func():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def start_gateway_server():
    ddfScript = os.path.join(DDF_HOME, "exe", "ddf_class.sh")
    print ddfScript
    command = [ddfScript, "py4j.GatewayServer", "--die-on-broken-pipe", "0"]
    print command
    proc = Popen(command, stdout = PIPE, stdin = PIPE, preexec_fn = preexec_func)
    # get the port of the GatewayServer
    port = int(proc.stdout.readline())

    class JavaOutputThread(Thread):
        def __init__(self, stream):
            Thread.__init__(self)
            self.daemon = True
            self.stream = stream

        def run(self):
            while True:
                line = self.stream.readline()
                sys.stderr.write(line)
    JavaOutputThread(proc.stdout).start()
    # connect to the gateway server
    gateway = JavaGateway(GatewayClient(port = port), auto_convert = False)
    java_import(gateway.jvm, "com.adatao.ddf.*")
    java_import(gateway.jvm, "com.adatao.ddf.spark.*")
    return gateway

