import com.adatao.pa.ddf.spark.DDFManager

object PAtest2 {

	def test(hostname: String) = {
		val mgr = DDFManager(hostname)
		val table = mgr.sql2ddf("select * from airline")
		println(s"table.nrow = ${table.nrow}") 
		println(s"table.ncol = ${table.ncol}")
		println(s"table.getColumnsNames = ${table.getColumnNames.mkString(", ")}")

		val table2 = table.project("arrdelay", "depdelay", "origin", "dayofweek", "cancelled")
		
    println(s"table2.getColumnsNames = ${table2.getColumnNames.mkString(", ")}")

		val table3 = table2.filter("origin=SFO")
		table3.fetchRows(10)

		val table4 = table2.groupBy(List("origin"), List("adelay = avg(arrdelay)"))
		table4.getColumnNames
		table4.top(List("adelay"), 10, "asc")

		table2.summary
		table2.fivenum

		table2.setMutable(true)

		table2.dropNA()
		table2.summary

		//##########
		//# ML
		//##########
		val kmeans = table3.ML.Kmeans(Array("arrdelay", "depdelay"), 3, 5)
		kmeans.predict(Array(24, 22))

		table2.setName("flightInfo")
	}
}
