package com.adatao.spark.ddf.ml

import java.util.HashMap
import org.jblas.DoubleMatrix
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import java.util.Arrays
import com.adatao.ddf.content.AMetaDataHandler.ICustomMetaData

class TransformRow(xCols: Array[Int]) extends Serializable {

  var mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]] = new HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]()
  var numNewColumns: Int = 0
  var numDummyCols = 0

  def initialize(customMetaData : HashMap[Integer, ICustomMetaData]) {
    //get mapping
    //get column mapping for dummyEncoding
    mapping = getDummyColumnMapping(customMetaData)
    if (mapping != null) {
      var iterator2 = mapping.keySet().iterator()
      while (iterator2.hasNext()) {
        numDummyCols += mapping.get(iterator2.next()).size() - 2
      }

      //update newColumns
      var iterator3 = mapping.keySet().iterator()
      while (iterator3.hasNext()) {
        numNewColumns += mapping.get(iterator2.next()).size() - 1
      }
    }
  }

  //TODO get dummy column mapping
  def getDummyColumnMapping(customMetaData: HashMap[Integer, ICustomMetaData]): HashMap[java.lang.Integer, HashMap[String, java.lang.Double]] = {

    //    //handle categorical columns
    //		if (hasCategoricalColumn) {
    //			println(">>>>>>>>>> hasCategoricalColumn")
    //			//if we don't have mapping yet then we will need to intilize and build one
    //			//by using metaInfo 
    //			//set map categorical columns value to Double by calling get muti factor
    //			if (!hasDummyMapping)
    //				init(Utils.getMapCategoricalColumnValueToDouble(dataContainer, categoricalCols))
    //		}
    //
    //		TODO Check number of features before building matrix

    null.asInstanceOf[HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]]
  }

  def hasCategoricalColumn(): Boolean = {
    return (mapping != null && mapping.size() > 0)
  }
  def hasCategoricalColumn(columnIndex: Int): Boolean = {
    (mapping != null && mapping.containsKey(columnIndex))
  }
  /*
	 * input column value "String"
	 * output: the mapping, double value
	 */
  def transform(columnIndex: Int, columnValue: String): Double = {
    if (mapping.containsKey(columnIndex)) {
      var v = mapping.get(columnIndex)
      if (v.containsKey(columnValue)) {
        v.get(columnValue)
      } else {
        System.err.println(">>>>> [Out of Dictionary] Exception while convert categorical column cell to double: original value = " + columnValue)
        -1.0
      }
    } else {
      System.err.println(">>>>>>>>>>> Exception mapping doesn't contains column index " + columnIndex + "\t columnvalue=" + columnValue)
      -1.0
    }
  }
  /*
   * input rows of double
   * return rows off double with extra dummy columns
   *
   */
  def transform(row: Matrix): DoubleMatrix = {
    var oldNumColumns = row.data.length

    //new columns = bias term + old columns + new dummy columns 
    var newRow = new Vector(xCols.length + 1 + numNewColumns)

    //bias term
    var oldColumnIndex = 0
    var originalColumnValue = row.get(oldColumnIndex)
    var newColumnIndex = 0
    var originalColumnIndex = 0
    var bitmap = null.asInstanceOf[Vector]

    newRow.put(newColumnIndex, originalColumnValue)

    oldColumnIndex = 1
    newColumnIndex = 1

    var j = 0

    while (oldColumnIndex < oldNumColumns && oldColumnIndex <= xCols.length) {

      originalColumnValue = row.get(oldColumnIndex)
      originalColumnIndex = xCols(oldColumnIndex - 1)

      //if normal double column
      //xCols(oldColumnIndex-1) because of bias-term
      if (!mapping.containsKey(originalColumnIndex)) {
        newRow.put(newColumnIndex, originalColumnValue) // x-feature #i
        newColumnIndex += 1
      } else {
        //bitmap vector
        bitmap = getNewRowFromCategoricalRow(originalColumnValue, originalColumnIndex)

        j = 0
        while (j < bitmap.length) {
          newRow.put(newColumnIndex, bitmap(j))
          j += 1
          newColumnIndex += 1
        }
      }

      oldColumnIndex += 1
    }

    //convert to one vector
    new DoubleMatrix(newRow.data).transpose()
  }

  /*
	 * from double column value to dummy vector
	 * 		input: column value in double
	 * 		input: dummy column length, #number of dummy column
	 *   
	 * return bitmap vector with i th index wi ll be 1 if column value = i 
	 */
  def getNewRowFromCategoricalRow(columnValue: Double, originalColumnIndex: Int): Vector = {
    //k-1 level
    var dummyCodingLength = mapping.get(originalColumnIndex).size - 1
    var ret: Vector = new Vector(dummyCodingLength)
    var j = 0
    var colVal = columnValue.asInstanceOf[Int]
    while (j < dummyCodingLength) {
      if (colVal != 0) {
        if (j == colVal - 1) {
          ret.put(j, 1.0)
        } else {
          ret.put(j, 0.0)
        }
      } else {
        ret.put(j, 0.0)
      }
      j += 1
    }
    ret
  }
}
