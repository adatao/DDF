package adatao.bigr.spark.execution

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Upload file from local source on server to HDFS
 *
 */
class UploadToHDFS(sourceFolder: String, destFolder: String, overwrite: Boolean) extends AExecutor[Unit]{

	protected override def runImpl(context: ExecutionContext): Unit = {
		val conf= new Configuration()
		val fs = FileSystem.get(conf)

		val hdfsPath = new Path(destFolder)
		val localPath = new Path(sourceFolder)
		if(fs.exists(hdfsPath)){
			if(overwrite){
				fs.delete(hdfsPath, true)
			}else
			throw new Exception("Folder already exists, use overwrite=TRUE if want to overwrite.")
		}
		fs.copyFromLocalFile(false, false, localPath, hdfsPath)

	}
}
