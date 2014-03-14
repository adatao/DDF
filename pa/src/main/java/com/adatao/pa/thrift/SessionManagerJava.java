package com.adatao.pa.thrift;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.thrift.types.ASessionThread;

@SuppressWarnings("unused")
public class SessionManagerJava {
	ConcurrentHashMap<String, ASessionThread> sidThreadMap = new ConcurrentHashMap<String, ASessionThread>();
	ConcurrentHashMap<String, ASessionThread> cidThreadMap = new ConcurrentHashMap<String, ASessionThread>();
	
	private int thriftPort = 7911;
	private int uiPort = 30001;
	private int driverPort = 20001;
	
	public static class Session {
		private String sessionID;
		private int thriftPort;
		private int uiPort;
		private int driverPort;
		
	}
	/**
	 * 
	 * @param sparkThread
	 * @param clientID
	 * @return
	 */
	public String addSession(ASessionThread sparkThread, String clientID) {
		String sessionID = UUID.randomUUID().toString();
		sidThreadMap.put(sessionID, sparkThread);
		if (clientID != null) {
			cidThreadMap.put(clientID, sparkThread);
		}
		return sessionID;
	}
	
	// this function is used by RWorkerCommandHandler which has sessionID given to it 
	// by the Server
	public void addSession(SparkThread sparkThread, String clientID, String sessionID) {
		sidThreadMap.put(sessionID, sparkThread);
		if (clientID != null) {
			cidThreadMap.put(clientID, sparkThread);
		}
	}
	
	public void removeSession(String sessionID){
		ASessionThread st = sidThreadMap.get(sessionID);
		if (st != null) {
			String clientID = st.getClientID();
			if (clientID != null)
				cidThreadMap.remove(clientID);
			sidThreadMap.remove(sessionID);
		}
	}
	
	private synchronized int getThriftPort(){
		return thriftPort++;
	}
	
	private synchronized int getUiPort(){
		return uiPort++;
	}
	
	private synchronized int getDriverPort(){
		return driverPort++;
	}
	
	public ASessionThread getSparkThreadBySid(String sessionID) {
		return sidThreadMap.get(sessionID);
	}

	public ASessionThread getSparkThreadByCid(String clientID) {
		return cidThreadMap.get(clientID);
	}

	public String getSessionID(String clientID) {
		return cidThreadMap.get(clientID).getSessionID();
	}

	public void stopSession(String sessionID) {
		if (sessionID == null){
			//TODO: log
			System.out.println("SessionID is null !!!!");
			return;
		}
		ASessionThread st = sidThreadMap.get(sessionID);
		if (st != null) {
			removeSession(sessionID);
			st.stopMe();
		}
	}
	
	public void stopAllSession() {
		for (String sessionID: sidThreadMap.keySet()) {
			ASessionThread st = sidThreadMap.get(sessionID);
			if (st != null) {
				String clientID = st.getClientID();
				if (clientID != null)
					cidThreadMap.remove(clientID);
				st.stopMe();
				sidThreadMap.remove(sessionID);
			}
		}
	}

	public Boolean hasClient(String clientID) {
		if (clientID == null){
			return false;
		}
		return cidThreadMap.containsKey(clientID);
	}
	
	public Boolean hasSession(String sessionID) {
		if (sessionID == null){
			return false;
		}
		return sidThreadMap.containsKey(sessionID);
	}

}
