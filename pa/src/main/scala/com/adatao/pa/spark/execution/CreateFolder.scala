//package com.adatao.pa.spark.execution

//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs._
//import java.io.IOException;
//import java.nio.file.FileAlreadyExistsException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 19/12/13
 * Time: 4:30 PM
 * To change this template use File | Settings | File Templates.
 */
//class CreateLocalFolder(tableName: String, dummy: String= null) extends AExecutor[String]{

//	protected override def runImpl(context: ExecutionContext): String = {
//		val conf = new Configuration()

//		var storageFolder= System.getProperty("bigr.storage", CreateLocalFolder.DEFAUL_STORAGE)
//		if(storageFolder.endsWith("/"))
//			storageFolder=storageFolder.take(storageFolder.length - 1)
//		//have to create a dummy file inside folder
//		val outFolderPath= Paths.get(storageFolder +"/" + tableName +"/" + tableName)
//		Files.createDirectories(outFolderPath.getParent)
//		val path = storageFolder + "/" + tableName + "/"
		/*
		val out= fs.create(outFolder)
		//delete dummy file
		fs.delete(outFolder, false)
		out.close() */
//		try{
//			Files.createFile(outFolderPath)
//		} catch {
//			case e: FileAlreadyExistsException => LOG.info("File already exists: " + e.getMessage)
//		}
//		path
//	}
//}

//object CreateLocalFolder{
//	val DEFAUL_STORAGE="/tmp/warehouse/"
//}
