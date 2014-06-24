package com.adatao.pa.rest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.execution.Connect;
import com.adatao.pa.spark.execution.ExecutionResultUtils;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.thrift.SessionManager;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@SuppressWarnings("serial")
public class CommandServlet extends HttpServlet {
	public static Logger LOG = LoggerFactory.getLogger(CommandServlet.class);
	SessionManager sessionManager;

	// lock for the Connect command, we want only one Connect command
	// is being executing at any moment in time
	private static final ReentrantLock connectLock = new ReentrantLock();

	public CommandServlet(SessionManager sessionManager) {
		this.sessionManager = sessionManager;
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		resp.setContentType("application/json");

		String[] parts = req.getRequestURI().split("/");

		if (parts.length == 0) {
			resp.sendError(400);
			return;
		}

		String command = parts[parts.length - 1];
		LOG.info(command);

		// Read params in JSON format

		String params = null;
		params = IOUtils.toString(req.getInputStream());
		;
		LOG.info(params);

		Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
				.create();

		Gson gson_output = new Gson();

		if (command.toLowerCase().equals("connect")) {
			try {
				Connect connect;
				if (params == null || params.equals("")) {
					connect = new Connect().setSessionManager(sessionManager);
				} else {
					connect = gson.fromJson(params, Connect.class)
							.setSessionManager(sessionManager);
				}

				connectLock.lock();
				JsonResult res = connect.run();
				connectLock.unlock();

				LOG.info(res.toString());

				resp.getWriter().println(gson_output.toJson(res));
			} catch (Exception e) {
				LOG.info("Exception: ", e);
				resp.getWriter().println(
						gson_output.toJson(new JsonResult()
								.setResult(new FailResult().setMessage(e
										.getMessage()))));
			}
		} else {
			String sid = null;
			sid = req.getQueryString().split("=")[1];
			LOG.info("Session id: " + sid);
			if (command.toLowerCase().equals("disconnect")) {
				try {
					LOG.info("Disconnect session " + sid);
					// sessionManager.stopSession(cmd.getSid());
					resp.getWriter().println(
							gson_output.toJson(new JsonResult().setSid(sid)));
				} catch (FileNotFoundException e) {
					LOG.info("Exception: ", e);
					resp.getWriter().println(
							gson_output.toJson(new JsonResult()
									.setResult(new FailResult().setMessage(e
											.getMessage()))));
				}
			} else {
				if (!sessionManager.hasSession(sid)) {
					resp.getWriter().println(
							gson_output.toJson(new JsonResult().setResult(new FailResult().setMessage("Session closed. Please reconnect!"))));
				}

				SparkThread sparkThread = (SparkThread) sessionManager.getSessionThread(sid);
				JsonResult res = null;
				try {
					ExecutionResult<?> exec_result = sparkThread.processJsonCommand1(new JsonCommand(sid, command, params));
				        res = new JsonResult().setResult(exec_result.toJson());
				} catch (Exception e) {
					LOG.error("Exception: ", e);
					Exception processedException = ExecutionResultUtils.processException(e);
					resp.getWriter().println(
							gson_output.toJson(new JsonResult().setResult(new FailResult().setMessage(processedException.getMessage()))));			
				}
				LOG.info("Returning result: " + res);
				resp.getWriter().println(gson_output.toJson(res));
			}
		}

	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		resp.sendError(405);
	}

	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		resp.sendError(405);
	}

	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		resp.sendError(405);
	}

}
