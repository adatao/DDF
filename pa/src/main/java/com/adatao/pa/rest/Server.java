package com.adatao.pa.rest;

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.adatao.pa.thrift.SessionManager;

public class Server {
	int port;
	String host = "localhost";
	org.eclipse.jetty.server.Server server;

	public Logger LOG = LoggerFactory.getLogger(Server.class);
	
	private static SessionManager sessionManager;

	public SessionManager getSessionManager() {
		return sessionManager;
	}

	// lock for the server control plane
	private ReentrantLock serverLock = new ReentrantLock();

	public Server(int port) {
		this.port = port;
		this.sessionManager = new SessionManager(port);
	}

	public void start() {
		LOG.info("Starting REST server on port " + port + "...");
		try {
			server = new org.eclipse.jetty.server.Server(port);
			ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
	        context.setContextPath("/");
	        server.setHandler(context);
	        
	        // entry for commands
	        // /command/<CommandName>[?sid=<SessionID>]
	        context.addServlet(new ServletHolder(new CommandServlet(sessionManager)), "/command/*");
	        
	        serverLock.lock();
	        server.start();
	        serverLock.unlock();
	        
	        server.join();
	        LOG.info("Started REST server on port " + port);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Cannot start server", e);
		}
	}

	public void stop() throws Exception {
		LOG.info("Stopping REST server on port " + port + "...");
		serverLock.lock();
		try {
			server.stop();
			LOG.info("Stopped REST server on port " + port);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Cannot stop server", e);
		}
		serverLock.unlock();
		
	}

	public static void main(String args[]) {
		int port = Integer.parseInt(System.getProperty("pA.server.restPort", "8088"));
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		}
		Server server = new Server(port);
		server.start();
	}
}
