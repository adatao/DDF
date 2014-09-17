package com.adatao.pa.spark

import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import com.adatao.ML.TCanLog
import com.adatao.pa.spark.types.ExecutionResult
import com.adatao.pa.spark.types.SuccessfulResult
import com.adatao.pa.thrift.types.ASessionThread
import java.util.concurrent.ArrayBlockingQueue
import com.adatao.pa.spark.types.FailedResult
import java.net.ServerSocket
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ConnectException

class MultiContextConnectResult(val host: String, val thriftPort: Int, val uiPort: Int, val clientID: String, val sessionID: String)

class MultiContextThread(val host: String, val resQueue: ArrayBlockingQueue[ExecutionResult[MultiContextConnectResult]]) {
//						extends ASessionThread with TCanLog {
//	var process: Process = null
//	var shutdownHook: Thread = null
//	val sparkHome: String = System.getProperty("spark.home", System.getenv("SPARK_HOME"));
//
//	override def run() {
//		LOG.info("Starting a new thread")
//
//		// Add a shutdown hook that kills executor on shutdown.
//		shutdownHook = new Thread() {
//			override def run() {
//				LOG.info("Shutdown hook killing thread session %s".format(session.sessionID))
//				stopProcess()
//			}
//		}
//		Runtime.getRuntime.addShutdownHook(shutdownHook)
//
//		runContext()
//	}
//
//	def buildCommandSeq(): Seq[String] = {
//		val script = if (System.getProperty("os.name").startsWith("Windows")) "spark-class.cmd" else "spark-class"
//		val runScript = new File(sparkHome, script).getCanonicalPath
//		Seq(runScript, "-Dname=BigRWorker-%s".format(clientID), 
//			"-Duser.name=%s".format(clientID),
//			"com.adatao.pa.thrift.BigRWorker",
//			session.thriftPort.toString, session.uiPort.toString, session.driverPort.toString, 
//			session.clientID, session.sessionID)
//	}
//
//	def runContext() {
//		
//		// Launch the process
//		session = sessionManager.addSession(this, clientID);
//		LOG.info("Starting new BigRWorker with thirtPort=%d, uiPort=%d, sessionID=%s"
//				.format(session.thriftPort, session.uiPort, session.sessionID))
//		
//		try {	
//			val command = buildCommandSeq()
//			val builder = new ProcessBuilder(command: _*)
//
//			// these perform redirection using OS native methods (i.e. dup syscall on Linux)
//			val fname = System.getProperty("java.io.tmpdir") + "/bigr." + clientID + ".out"
//			LOG.info("Child stdout/stderr will be redirected to: {}", fname)
//			val logFile = new File(fname)
//			// FIXME: currently shows mysterious compile error: builder.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
//			// FIXME: builder.redirectError(ProcessBuilder.Redirect.appendTo(logFile))
//
//			LOG.info("BigRWorker execution command: "+command.toString)
//				
//			process = builder.start()
//			
//			//Wait for the replies to make sure the client started
// 			var portOpen = false
// 			while(!portOpen) {
//				try {
//					val socket = new Socket("localhost", session.thriftPort)
//					portOpen = true
//				} catch {
//					case e: ConnectException => {
//						portOpen = false
//						LOG.info("BigRWorker is not yet listening at port %d, waiting ...".format(session.thriftPort))
//						Thread.sleep(100)
//					}
//				}
//				
// 			}
//			
//			LOG.info("BigRWorker is ready at port %d".format(session.thriftPort))
//			resQueue.put(new SuccessfulResult(new MultiContextConnectResult(host, session.thriftPort, session.uiPort,
//					session.clientID, session.sessionID)))
//		} catch {
//			case e: Exception => {
//				resQueue.put(new FailedResult(e.getMessage(), 
//						new MultiContextConnectResult(host, session.thriftPort, session.uiPort, session.clientID, session.sessionID)))
//				LOG.error("Error in thread session %s\n".format(session.sessionID))
//				LOG.error(e.getMessage(), e)
//				stopProcess()
//				Runtime.getRuntime.removeShutdownHook(shutdownHook)
//			}
//		}
//			
//		try {	
//			val exitCode = process.waitFor()
//			LOG.info("SparkContext process at port %d exited with code %d".format(session.thriftPort, exitCode))
//			sessionManager.stopSession(Some(session));
//		} catch {
//			case interrupted: InterruptedException =>
//				LOG.info("Runner thread session %s interrupted".format(session.sessionID))
//				stopProcess()
//				Runtime.getRuntime.removeShutdownHook(shutdownHook)
//
//			case e: Exception => {
//				LOG.error("Error in thread session %s\n".format(session.sessionID))
//				LOG.error(e.getMessage(), e)
//				stopProcess()
//				Runtime.getRuntime.removeShutdownHook(shutdownHook)
//			}
//		}
//	}
//	
//	def stopMe() {
//		stopProcess()
//		interrupt()
//	}
//	
//	/** Stop the worker process */
//	def stopProcess() {
//		if (process != null) {
//			process.destroy()
//			process.waitFor()
//		}
//	}

} 
