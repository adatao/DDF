package adatao.bigr.spark.execution

import adatao.bigr.spark.types.ABigRClientTest

class RDirMapperSuite extends ABigRClientTest {
	test("FileMapReduce") {
		var fmr = new RDirMapper(
				"args<-commandArgs(TRUE)\n" +
				"inputFile <- args[1]\n" +
				"outputDir <- args[2]\n" +
				"outputFile <- paste(outputDir, \"outfile\", sep=\"/\")\n" +
				"con <- file(inputFile, \"rt\")\n" + 
				"ln <- readLines(con, 1)\n" +
				"close(con)\n" +
				"file.create(outputFile)\n" +
				"con <- file(outputFile, \"wt\")\n" +
				"write(ln, con)\n" +
				"close(con)\n",
				"/tmp/inputtest", 
				"/tmp/outputtest")
		bigRClient.execute[Unit](fmr)
	}
}
