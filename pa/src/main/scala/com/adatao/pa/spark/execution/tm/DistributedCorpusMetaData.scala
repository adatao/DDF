package adatao.bigr.spark.execution.tm

/**
 * @author Cuong Kien Bui
 * @version 0.1
 */
class DistributedCorpusMetaData(val createdDate: String = null,
    val creator: String = null,
    val description: String = null,
    val lang: String = "en",
    val numDocs: Int) extends Serializable {
}