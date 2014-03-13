package adatao.bigr.spark.execution.tm

import java.util.HashMap

class DirectoryDataSource(params: HashMap[String, String]) extends TextDataSource {
  val sourcePath: String = params.get("path")
}
