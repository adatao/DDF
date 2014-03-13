/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package adatao.bigr.spark.execution

import scala.sys.process._
import scala.None
import java.util.UUID
import java.io.File
import java.io.FileWriter
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SerializableWritable
import java.util.Arrays
import adatao.bigr.spark.types.FailedResult

class RDirMapper(val mapScript: String, val inputPath: String, val outputPath: String) extends AExecutor[Unit] {
	//TODO: broadcast mapScript and reduceScript
	
	def pathValidate(fileSystem: FileSystem, inputPath: Path, outputPath: Path): Boolean = {
		fileSystem.exists(inputPath) && fileSystem.exists(outputPath) &&
		fileSystem.getFileStatus(inputPath).isDir() && fileSystem.getFileStatus(outputPath).isDir()
	}
	
	def runImpl(context: ExecutionContext): Unit = {
		val sparkContext = context.sparkContext
		val bcMapScript: Broadcast[String] = sparkContext.broadcast(mapScript)
		val bcHadoopConfig: Broadcast[SerializableWritable[Configuration]] = sparkContext.broadcast(new SerializableWritable(context.hadoopConfig))
		val input = new Path(inputPath)
		val output = new Path(outputPath)
		
		/*
		 * This is file system where data input/output files are stored.
		 * It can be either HDFS or local file system depending on the configuration
		 * inputPath and outputPath belong to this file system
		 */
		val fileSystem = input.getFileSystem(context.hadoopConfig)
		
		//TODO: complete input validation
		if (!pathValidate(fileSystem, input, output)){
			LOG.info("Either %s or %s does not exist".format(inputPath, outputPath))
			return
		}
		
		val fileListRDD = sparkContext.parallelize(fileSystem.listStatus(input).map(_.getPath.toString).toList)
		
		// a better function to use here is .collect(), but it did not work when map function returns Unit
		// so use .count() instead
		fileListRDD.map(f => RDirMapper.dirMapper(bcMapScript, f, outputPath, bcHadoopConfig)).count()
	}
}

object RDirMapper {
	
	def execRScript(scriptPath: String, args: List[String] = null) = {
		var argsNonEmpty = List[String]()
		if (args != null) argsNonEmpty = args
		(Seq("Rscript", scriptPath) ++ args).! == 0
	}

	def dirMapper(bcMapScript: Broadcast[String], inputFile: String, outputPath: String, 
			bcHadoopConfig: Broadcast[SerializableWritable[Configuration]]): Unit = {
		val mapScript = bcMapScript.value
		val hadoopConfig = bcHadoopConfig.value.value
		val inputFilePath = new Path(inputFile)
		val fileSystem = inputFilePath.getFileSystem(hadoopConfig)

		/*
		 * This working dir is on the local file system, where we store R scripts and temp files
		 * produced by the R scripts
		 */
		//TODO: find a better workingDir
		val workingDir = new File("/mnt/tmp/work/", UUID.randomUUID.toString)
		val outputDir = new File(workingDir, "output")
		if (outputDir.mkdirs()) {
			try {
				// LOG.info("Working dir: "+workingDir.getPath)
				// write mapScript to workingDir
				if (mapScript != null) {
					val mapScriptFile = new File(workingDir, "mapScript.R")
					//	LOG.info("Write map script: " + mapScriptFile.getPath)
					val out = new FileWriter(mapScriptFile)
					out.write(mapScript)
					out.close

					//download inputFile to workingDir
					fileSystem.copyToLocalFile(inputFilePath, new Path(workingDir.getPath))
					execRScript(mapScriptFile.getPath, 
							List(workingDir.getPath+"/"+inputFilePath.getName, outputDir.getPath))
					fileSystem.copyFromLocalFile(false, true, outputDir.list().map(f => new Path(outputDir.getPath(), f)), new Path(outputPath))
				}
				FileUtils.deleteDirectory(workingDir)
			} catch {
				case e: Exception =>
					if (workingDir.exists()) {
						FileUtils.deleteDirectory(workingDir)
					}
					throw (e)
			}
		}
	}
}
