package com.adatao.pa.thrift.types;

import adatao.bigr.thrift.Session;
import adatao.bigr.thrift.SessionManager;

public abstract class ASessionThread extends Thread {
	protected String clientID;
	protected String sessionID;
	protected Session session;
	protected Boolean isShark = true;
	protected SessionManager sessionManager;
	
	public abstract void stopMe();
	
	public String getClientID() {
		return clientID;
	}
	public ASessionThread setClientID(String clientID) {
		this.clientID = clientID;
		return this;
	}

	public ASessionThread setShark(Boolean isShark) {
		this.isShark = isShark;
		return this;
	}

	public ASessionThread setSessionManager(SessionManager sessionManager) {
		this.sessionManager = sessionManager;
		return this;
	}

	public String getSessionID() {
		return sessionID;
	}

	public ASessionThread setSessionID(String sessionID) {
		this.sessionID = sessionID;
		return this;
	}
	
}
