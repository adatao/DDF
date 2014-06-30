import com.adatao.pa.ddf.spark.DDFManager

object paTest {

  def test() = {
    val mng = DDFManager.get("spark")

    val ddf = mng.sql2ddf("select * from airline")
    ddf.nrow()
    ddf.ncol()
    ddf.getColumnNames()
    ddf.summary()
    ddf.fivenum()

    val ddf2 = ddf.project("arrdelay", "depdelay", "arrtime", "origin", "distance")
    ddf2.nrow()
    ddf2.ncol()
    ddf2.getColumnNames()

    val ddf3 = ddf2.filter("origin == SFO")
    ddf3.nrow()
    ddf3.ncol()
    ddf3.fetchRows(10)

    val ddf4 = ddf2.groupBy(List("origin"), List("avg_arrdelay=avg(arrdelay)"))
    ddf4.nrow()
    ddf4.fetchRows(10)
    ddf4.top(List("avg_arrdelay"), 10, "asc")

    ddf2.setMutable(true)
    ddf2.nrow()
    ddf2.dropNA()
    ddf2.nrow()

    ddf3.binning("distance", "equalInterval", 3)
    ddf.fetchRows(10)
    val kmeans = ddf2.ML.Kmeans(Array("arrdelay", "depdelay"), 3, 10)
    kmeans.predict(Array(24, 22))

    ddf2.transform("delayed=if(arrdelay>10.89, 1, 0)")
    val ddf5 = ddf2.project("depdelay", "distance", "delayed")
    val model = ddf5.ML.LogisticRegression(Array("depdelay","distance"), "delayed")
    model.predict(Array(0.1, 0.5))

    ddf.setName("flightInfo")
  }
}
