import scala.io.Source
import java.io._

object converter {
    
    def convertFile(file: String, out: String) = {
        val w = new BufferedWriter(new FileWriter(out))
        for(line <- Source.fromFile(file).getLines()) {
            val str = line.split(",").map{str => hash(str)}.mkString(",")
            w.write(str + "\n") 
        }
        w.close()    
    }

    def convertFile2(file: String, out: String) = {
        val w = new BufferedWriter(new FileWriter(out))
        for(line <- Source.fromFile(file).getLines()) {
            val ls = line.split(",")
            val (ls1, ls2) = ls.splitAt(2)
            println(">ls1 = " + ls1)
            println(">ls2 = " + ls2)
            val ls11 = ls1.map{str => hash(str)}
            val str = (ls11 ++ ls2).mkString(",")
            w.write(str + "\n")
        }
        w.close()    
    }

    def hash(str: String): Long = {
            var h = 1125899906842597L
                var i = 0
                    while(i < str.length) {
                              h = 31 * h + str.charAt(i)
                                    i += 1
                                        }
                                            h
                                              }
}
