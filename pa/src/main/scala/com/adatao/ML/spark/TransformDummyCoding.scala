package com.adatao.ML.spark

import java.util.ArrayList
import java.util.HashMap
import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import com.adatao.pa.spark.DataManager.MetaInfo
import com.adatao.pa.spark.DataManager.DataContainer
import com.adatao.ML.Utils
import com.adatao.pa.spark.DataManager
import org.apache.spark.rdd.RDD
import com.adatao.ML.ALinearModel
import com.adatao.ML.TModel
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import scala.collection.mutable.ListBuffer
import org.jblas.DoubleMatrix

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.util

class TransformDummyCoding(metaInfo: Array[MetaInfo], xCols: Array[Int], var mapReferenceLevel: HashMap[String, String] = null) extends Serializable {
	val LOG: Logger = LoggerFactory.getLogger(this.getClass)
	var dummyColumnMapping = new HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]()
	var _mapReferenceLevel = new HashMap[java.lang.Integer, String]()
	var categoricalCols = new Array[Int](0)
	setCategoricalColumnIndex()
	var numDummyCols = 0
	
	//convert column_name, reference value to column_index to reference_value
	//example: client send (column name, reference_level) 
	//we convert to (column_index, reference_level) 
	//note that the client will send an identical column name  and internally we convert to colum index (0-based)
	
	//input: "{ (v17, ISP), (v18, LAS) }"   
	//output:"{ (16, ISP), (17, LAS) }"
	if(mapReferenceLevel != null) {
		println(">>>>> mapReferenceLevel is not null " + mapReferenceLevel)
		var columnIndex = 0
		var i = 0
		//get dummyColumn mapping by using metaInfo which is available while running training step
		if (metaInfo != null) {
			assert(xCols != null)
			assert(xCols.length > 0)
			var columnIndex = xCols(0)
			var mInfo = metaInfo(columnIndex)
			while (i < xCols.length) {
				columnIndex = xCols(i)
				assert(metaInfo.length > columnIndex)
				mInfo = metaInfo(columnIndex)
				if (mInfo != null) {
					//categorical if column type string
					//OR user already transform dataframe by calling as.factor("column_name")
					if(mapReferenceLevel.containsKey(mInfo.getHeader())) {
						_mapReferenceLevel.put(columnIndex, mapReferenceLevel.get(mInfo.getHeader()))
					}
				}
				else System.err.println("exception: can't find metainfo for column index: " + columnIndex)
				i += 1
			}
		}
	}
		

	//constructor, input is model and xCols
	def this(model: TModel, xCols: Array[Int]) = {
		this(null.asInstanceOf[Array[MetaInfo]], xCols)
		this.mapReferenceLevel = model.asInstanceOf[ALinearModel[Double]].mapReferenceLevel
		this.dummyColumnMapping = model.asInstanceOf[ALinearModel[Double]].dummyColumnMapping
		updateNumDummyColumns

		this
	}
	
	//set reference level for categorical column, this API is provided for back-end developer
	//column index is zero-based
	def setReferenceLevel(column: String, referenceValue: String) {
		mapReferenceLevel.put(column, referenceValue)
	}
	def setReferenceLevel(map: HashMap[String, String]) {
		mapReferenceLevel = map
	}

	/*
	 * k unique values returns k-1 independent level
	 * 
	 */
	def updateNumDummyColumns {
		//update numDummyCols
		//number of NEW dummy columns only
		var iterator2 = dummyColumnMapping.keySet().iterator()
		while (iterator2.hasNext()) {
			numDummyCols += dummyColumnMapping.get(iterator2.next()).size() - 2
		}
	}

	/*
	 * input: sharkdataFrame or normal dataframe
	 * output: 
	 * 	matrix: adding new dummy columns
	 *  dummyCoding: mapping of dummy coding
	 * 
	 */
	def transform(dataContainer: DataContainer, yCol: Int): (RDD[(Matrix, Vector)], HashMap[java.lang.Integer, HashMap[java.lang.String, java.lang.Double]]) = {
		//handle categorical columns
		if (hasCategoricalColumn) {
			println(">>>>>>>>>> hasCategoricalColumn")
			//if we don't have mapping yet then we will need to intilize and build one
			//by using metaInfo 
			//set map categorical columns value to Double by calling get muti factor
			if (!hasDummyMapping)
				init(Utils.getMapCategoricalColumnValueToDouble(dataContainer, categoricalCols))
		}

		//Check number of features before building matrix
		val nonCategorialColumns = xCols.toSet diff categoricalCols.toSet
		var countFeatures= 0
		dummyColumnMapping.foreach {
			case(idx, map) =>  countFeatures += map.size - 1
		}
		//add nonCategorialColumns and add 1 for bias term
		countFeatures += nonCategorialColumns.size + 1

		val maxNumFeatres= System.getProperty("bigr.lm.maxNumFeatures", ALinearModel.MAXNUMFEATURES_DEFAULT.toString).toInt
		LOG.info("countFeatures = " + countFeatures)
		LOG.info("Max number of features= " + maxNumFeatres)
		if(countFeatures > maxNumFeatres)
			throw new IllegalArgumentException("Request to build model with %d features".format(countFeatures) +
				", however cluster is configured to allow maximum of %d features only".format(maxNumFeatres))


		if (hasDummyMapping) {
			println(">>>>>>>>>> hasDummy")
			var dataPartition: RDD[(Matrix, Vector)] = null
			//get data partition to Matrix, categorical column String values are converted to unique value double

			if (dataContainer.getType == ContainerType.SharkDataFrame) {
				dataPartition = dataContainer.asInstanceOf[DataManager.SharkDataFrame].getDataTableCategorical(xCols, yCol, dummyColumnMapping)
			}
			else {
				dataPartition = dataContainer.getRDD.rdd.filter { row =>
					row(yCol) != null && xCols.map(i => row(i)).forall(x => x!= null)
				}.mapPartitions(
					rowsToPartitionMapperCategorical(xCols, yCol)
				).filter { Xy =>
					(Xy._1.columns > 0) && (Xy._2.rows > 0)
				}
			}
			//convert to dummy column
			dataPartition = dataPartition.map(instrument)
			(dataPartition, dummyColumnMapping)
		}
		else {
			println(">>>>>>>>>> don't have dummy")
			var dataPartition: RDD[(Matrix, Vector)] = null
			if (dataContainer.getType == ContainerType.SharkDataFrame) {
				dataPartition = dataContainer.asInstanceOf[DataManager.SharkDataFrame].getDataTable(xCols, yCol)
			}
			else {
				dataPartition = dataContainer.getRDD.rdd.filter { row =>
					row(yCol) != null && xCols.map(i => row(i)).forall(x => x!= null)
				}.mapPartitions(
					Utils.rowsToPartitionMapper(xCols, yCol)
				).filter { Xy =>
					(Xy._1.columns > 0) && (Xy._2.rows > 0)
				}
				(dataPartition, dummyColumnMapping)
			}
			(dataPartition, dummyColumnMapping)
		}
	}

	/* 
	 * input: categoricalColumnSize is the mapping between column id and a number of unique values in categorical column 
	 * 		key = original column id of X
	 * 		value = length of dummy column, including original one 
	 * input: original matrix
	 * output: new matrix with new dummy columns
	 */
	def instrument[InputType](inputRow: (Matrix, Vector)): (Matrix, Vector) = {

		//so we need to do minus one for original column
		var oldX = inputRow._1
		var oldY = inputRow._2

		//add dummy columns
		val numCols = oldX.columns
		var numRows = oldX.rows

		//this is the most critical improvement to avoid OOM while building lm-categorical
		//basically we don't create a new matrix but rather updating value in-place
		
//		val newX = new Matrix(numRows, numCols + numDummyCols)
//		var newColumnMap = new Array[Int](numCols + numDummyCols)
		
		
		//new code, coulmnMap has same dimensions with input matrix columns
		var newColumnMap = new Array[Int](numCols)

		//row transformer
		var trRow = new TransformRow(xCols, dummyColumnMapping)

		//for each row
		var indexRow = 0
		var currentRow = null.asInstanceOf[Matrix]
		var newRowValues = null.asInstanceOf[DoubleMatrix]
		while (indexRow < oldX.rows) {

			//for each rows
			currentRow = Matrix(oldX.getRow(indexRow))

			newRowValues = trRow.transform(currentRow)
			//add new row
			oldX.putRow(indexRow, newRowValues)

			//convert oldX to new X
			indexRow += 1
		}

//		println("after dummy coding, X = " + util.Arrays.deepToString(oldX.toArray2.asInstanceOf[Array[Object]]))

		(oldX, oldY)
	}

	/*
	 * input: normal dataframe
	 */
	def rowsToPartitionMapperCategorical(xCols: Array[Int], yCol: Int)(inputRows: Iterator[Array[Object]]): Iterator[(Matrix, Vector)] = {
		val rows = new ListBuffer[Array[Object]]
		var numRows = 0
		while (inputRows.hasNext) {
			val aRow = inputRows.next
			if (aRow != null) {
				rows.append(aRow)
				numRows += 1
			}
		}

		assert(dummyColumnMapping != null)

		val numCols = xCols.length + 1 // 1 bias term + n x-features
		val Y = new Vector(numRows)
//		val X = new Matrix(numRows, numCols)
		val X = new Matrix(numRows, numCols + numDummyCols)
		
		val trRow = new TransformRow(xCols, dummyColumnMapping)

		var row = 0
		rows.foreach(inputRow ⇒ {
			X.put(row, 0, 1.0) // bias term

			var i = 1
			var columnIndex = 0
			var columnValue = ""
			var newValue: Double = -1.0

			while (i < numCols) {
				columnIndex = xCols(i - 1)

				//if this column is categorical column
				if (trRow.hasCategoricalColumn() && trRow.hasCategoricalColumn(columnIndex)) {
					columnValue = inputRow(columnIndex) + ""
					newValue = trRow.transform(columnIndex, columnValue)
				}
				if (newValue == -1.0)
					newValue = Utils.objectToDouble(inputRow(columnIndex))

				X.put(row, i, newValue) // x-feature #i
				newValue = -1.0  // TODO: dirty and quick fix, need proper review
				i += 1
			}

			Y.put(row, Utils.objectToDouble(inputRow(yCol))) // y-value
			row += 1
		})
		//		LOG.info("X is %s".format(X.toString))
		//		LOG.info("Y is %s".format(Y.toString))

		Iterator((X, Y))
	}

	/*
	 * input: [1, {(IAD, 1.0), (IND , 1.0), (IPD, 1.0)}]
	 * output: [1, {(IAD, 1.0), (IND , 2.0), (IPD, 3.0)}]
	 */
	def init(map: HashMap[java.lang.Integer, HashMap[String, java.lang.Double]]) {
		var iterator = map.keySet().iterator()

		
		//for each original columns index
		while (iterator.hasNext()) {
			var columnIndex = iterator.next()
			var currentColumn = map.get(columnIndex)
			var currentReferenceMap =  ""
				
			if (_mapReferenceLevel!= null && _mapReferenceLevel.containsKey(columnIndex))
				currentReferenceMap =   _mapReferenceLevel.get(columnIndex)
			
			var newColumn = new HashMap[String, java.lang.Double]
			var it2 = currentColumn.keySet().iterator()
			
			var tempValue = ""
			var tempIndex = 0.0
			var i: java.lang.Double = 0.0
			while (it2.hasNext()) {
				var columnValue = it2.next()
				
				if(i==0.0) tempValue = columnValue
				//if reerence level, set to zero
				if(currentReferenceMap!="" && currentReferenceMap.toLowerCase().trim().equals(columnValue.toLowerCase().trim())) {
					newColumn.put(columnValue, 0.0)
					tempIndex = i
					
					newColumn.remove(tempValue)
					newColumn.put(tempValue, tempIndex)
				}
				else
				newColumn.put(columnValue, i)
				
				i += 1.0
			}
			//sort current column
			dummyColumnMapping.put(columnIndex, newColumn)
		}

		updateNumDummyColumns
		dummyColumnMapping

	}

	/*
	 * input: dataContainer 
	 * input: array of original column index
	 * output: array of categorical column index, a subset of original column index
	 */
	def setCategoricalColumnIndex() {

		var categoricalIndex: ArrayList[Int] = new ArrayList[Int]()
		var columnIndex = 0
		var i = 0
		//get dummyColumn mapping by using metaInfo which is available while running training step
		if (metaInfo != null) {
			assert(xCols != null)
			assert(xCols.length > 0)
			var columnIndex = xCols(0)
			var mInfo = metaInfo(columnIndex)
			while (i < xCols.length) {
				columnIndex = xCols(i)
				assert(metaInfo.length > columnIndex)
				mInfo = metaInfo(columnIndex)
				if (mInfo != null) {
					//categorical if column type string
					//OR user already transform dataframe by calling as.factor("column_name")
					if (mInfo.getType().toLowerCase().trim().equals("string") || mInfo.getType().toLowerCase().trim().equals("java.lang.string") || mInfo.hasFactor()) {
						categoricalIndex.add(xCols(i))
					}
				}
				else System.err.println("exception: can't find metainfo for column index: " + columnIndex)
				i += 1
			}
		}
		//get dummyColumn mapping by using model information while running model prediction/evaluation/etc
		else if (dummyColumnMapping != null && dummyColumnMapping.size > 0) {
			var iter = dummyColumnMapping.keySet().iterator()
			var index = 0
			while (iter.hasNext()) {
				index = iter.next()
				categoricalIndex.add(index)
			}
		}
		//
		var j = 0
		categoricalCols = new Array[Int](categoricalIndex.size())
		while (j < categoricalIndex.size()) {
			categoricalCols(j) = categoricalIndex.get(j)
			j += 1
		}
		println(">>>>>>>>>>>>>.. number of categorical=" + j)
	}

	/*
	 * return true if we have categorical columns, false if otherwise
	 * 
	 */
	def hasCategoricalColumn(): Boolean = {
		return (dummyColumnMapping.size() > 0 || categoricalCols.length > 0)
	}

	/*
	 * return true if we have categorical columns, false if otherwise
	 * 
	 */
	def hasDummyMapping(): Boolean = {
		return (dummyColumnMapping != null && dummyColumnMapping.size() > 0)
	}

}
