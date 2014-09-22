package com.adatao.pa.thrift

import java.util.UUID

import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedMap

import com.adatao.ML.TCanLog
import com.adatao.pa.thrift.types.ASessionThread

class Session(val sessionThread: ASessionThread, val sessionID: String, val clientID: String, val thriftPort: Int, 
		val uiPort: Int, val driverPort: Int) {
	def sessionID_ = sessionID
	def clientID_ = clientID
	def thriftPort_ = thriftPort
	def uiPort_ = uiPort
	def driverPort_ = driverPort
	def sessionThread_ = sessionThread
}

class SessionID(val sessionID: String) {
	def id = sessionID
}

class ClientID(val sessionID: String) {
	def id = sessionID
}

class SessionManager(var currentThriftPort: Int) extends TCanLog {
	private val sidThreadMap = new HashMap[String, Session]() with SynchronizedMap[String, Session] 
	private val cidThreadMap = new HashMap[String, Session]() with SynchronizedMap[String, Session]
	
	private var currentUIPort = System.getProperty("bigr.multiuser.server.initialUIPort", "30001").toInt;
	private var currentDriverPort = System.getProperty("bigr.multiuser.server.initialDriverport", "20002").toInt;
	
	def getNewThriftPort = this.synchronized { currentThriftPort+=1; currentThriftPort }
	def getNewUIPort = this.synchronized { currentUIPort+=1; currentUIPort }
	def getNewDriverPort = this.synchronized { currentDriverPort+=1; currentDriverPort }
	

	/**
	 * This function is intended to use at Server in MultiUser/MultiContext mode 
	 * New thriftPort, uiPort, driverPort will be generated
	 * @param sessionThread
	 * @param clientID
	 * @return
	 */
	def addSession(sessionThread: ASessionThread, clientID: String): Session = {
		val sessionID = UUID.randomUUID().toString();
		val myClientID = if (clientID == null) SessionManager.ANONYMOUS else clientID
		
		val session = new Session(sessionThread, sessionID, myClientID, getNewThriftPort, getNewUIPort, getNewDriverPort)
		return addSession(session)
	}
	
	def addSession(sessionThread: ASessionThread, clientID: String, thriftPort: Int, uiPort: Int, driverPort: Int): Session = {
		val sessionID = UUID.randomUUID().toString();
		return addSession(sessionThread, sessionID, clientID, thriftPort, uiPort, driverPort)
	}
	
	def addSession(sessionThread: ASessionThread, sessionID: String, clientID: String, thriftPort: Int, uiPort: Int, driverPort: Int): Session = {
		val myClientID = if (clientID == null) SessionManager.ANONYMOUS else clientID
		val session = new Session(sessionThread, sessionID, myClientID, thriftPort, uiPort, driverPort)
		return addSession(session)
	}
	
	def addSession(session: Session): Session = {
		sidThreadMap.put(session.sessionID, session);
		cidThreadMap.put(session.clientID, session);
		return session;
	}
	
	def getSessionID(clientID: String): String = {
		cidThreadMap.get(clientID) match {
			case Some(s) => s.sessionID
			case None => null
		}
	}
	
	def getSessionThread(sessionID: String): ASessionThread = {
		sidThreadMap.get(sessionID) match {
			case Some(s) => s.sessionThread
			case None => null
		}
	} 
	
	def getSession(sessionID: String): Session = {
		sidThreadMap.get(sessionID) match {
			case Some(s) => s
			case None => null
		}
	}
	
	def stopSession(sessionID: String): Unit = {
		if (sessionID != null){
			val session = sidThreadMap.get(sessionID)
			stopSession(session)
		} else {
			LOG.info("SessionID is null !!!!");
		}
	}
	
	def stopSession(session: Option[Session]): Unit = {	
		session match {
			case Some(s) => cidThreadMap.remove(s.clientID)
							sidThreadMap.remove(s.sessionID) 
							s.sessionThread.stopMe()
			case None => null
		}
	}
	
	def stopAllSession() {
		sidThreadMap.foreach(s => stopSession(Some(s._2)))
	}

	def hasClient(clientID: String): Boolean = {
		if (clientID == null) cidThreadMap.contains(SessionManager.ANONYMOUS) else cidThreadMap.contains(clientID)
	}
	
	def hasSession(sessionID: String): Boolean = {
		if (sessionID == null){
			return false;
		}
		sidThreadMap.contains(sessionID);
	}

}

object SessionManager {
	val ANONYMOUS = System.getProperty("bigr.defaultuser", "anonymous")
}