package com.adatao.ML.spark

import java.util.HashMap
import org.jblas.DoubleMatrix
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import java.util.Arrays
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransformRow(xCols: Array[Int], mapping: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]) extends Serializable {

  val mLog: Logger = LoggerFactory.getLogger(this.getClass());

  var numNewColumns: Int = 0
  var iterator2 = mapping.keySet().iterator()
  while (iterator2.hasNext()) {
    numNewColumns += mapping.get(iterator2.next()).size() - 1
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
        mLog.error(">>>>> [Out of Dictionary] Exception while convert categorical column cell to double: original value = " + columnValue)
        -1.0
      }
    } else {
      mLog.error(">>>>>>>>>>> Exception mapping doesn't contains column index " + columnIndex + "\t columnvalue=" + columnValue)
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
