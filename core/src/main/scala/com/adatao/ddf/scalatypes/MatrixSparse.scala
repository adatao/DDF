package com.adatao.ddf.scalatypes

import no.uib.cipr.matrix.sparse.CompRowMatrix
import org.jblas.DoubleMatrix
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix
import no.uib.cipr.matrix.DenseVector
import no.uib.cipr.matrix.DenseMatrix
import no.uib.cipr.matrix.AbstractDenseMatrix


class MatrixSparse (numRows: Int, numCols: Int)  {
	
	println("[MatrixSparse] Setting up numRows=" + numRows + "\t numCols =" + numCols)
	var crs: FlexCompRowMatrix = new FlexCompRowMatrix(numRows, numCols)
	
	/*
	 * matrix Vector multiplication
	 * input: adatao vector, column vector
	 * output: DoubleMatrix
	 * TO DO: optimize crs.mult
	 */
	def mmul(other: Vector) : DoubleMatrix = {
		
		//print dimension
		println(">>>>>>>>>>>>> mmul Vector: number of rows \t:" + other.getRows() + "\t columns:" + other.getColumns())
		println(">>>>>>>>>>>>> mmul Matrix sparse: numRows=" + numRows + "\tnumCols=" + numCols)
		
		//check dimension
		if(numCols != other.getRows()) {
			System.err.println(">>>>>>>>>>> Wrong dimension: matrix: " + numRows + " * "  + numCols)
			sys.exit(1)
		}
		
		//convert to DenseMatrix, n rows, 1 column
		val denseMatrix: DenseMatrix = new DenseMatrix (new DenseVector(other.data))
		
		println(">>>> denseMatrix numRows  = " + denseMatrix.numRows() + "\t num columns=" + denseMatrix.numColumns())
		
		var retMatrix: DenseMatrix = new DenseMatrix(crs.numRows(), denseMatrix.numColumns())
//		
		println(">>>> retMatrix size  = " + retMatrix.numRows() + "\t" + retMatrix.numColumns())
		
		
		val startTime = System.currentTimeMillis()
		
		crs.mult(denseMatrix, retMatrix)
		
		val endTime = System.currentTimeMillis()
		println(">>>>>>>>>>>>>>>>>> timing mmul after mmul vector: " + (endTime-startTime))
		
		MatrixSparse.toDoubleMatrix(retMatrix)
		
	}
	
	def print() {
		val str = "matrix: " + numRows + " * "  + numCols + "\n " + this.crs.toString()
		println(str)
	}
	
	/*
	 * input: DoubleMatrix (implicitly vector)
	 * output: MatrixSparse
	 * assumption: dmatrix is dense matrix AND small enough to store in memory,  
	 * TO DO: optimize to copy block by block
	 * 
	 * time complexity is similar "upper bound" for CRS.mult(densematrix)
	 * CURRENT: not using
	 */
	def mmul_notused(dmatrix: DoubleMatrix) : DenseMatrix = {
		
		//returned matrix
		//m * n  multiply n * p return m * p
		val retMatrix = new DenseMatrix(this.numRows, dmatrix.getColumns())
		
		val startTime = System.currentTimeMillis()
		
		//first: converted to dense matrix
		val convertedMatrix = new DenseMatrix(dmatrix.getRows(), dmatrix.getColumns())
		var rows = 0
		var columns = 0
		while(rows < dmatrix.getRows()) {
			columns = 0
			while(columns < dmatrix.getColumns()) {
				convertedMatrix.set(rows, columns, dmatrix.get(rows, columns))
				columns += 1
			}
			rows += 1
		}
		
		val endTime = System.currentTimeMillis()
		println(">>>>>>>>>>>>>>>>>> timing mmul after convert: " + (endTime-startTime))
		

		//second: multiply CRS sparse matrix with dense matrix 
		println(">>>>>>>>> multiplying matrix: crs: numrwos=" + crs.numRows() + "\t numColumns=" + crs.numColumns() + "\tconvertedMatrix numrows=" + convertedMatrix.numRows() + "\t numCOlumns=" + convertedMatrix.numColumns() )
		crs.mult(convertedMatrix, retMatrix)
		
		val endTime2 = System.currentTimeMillis()
		println(">>>>>>>>>>>>>>>>>> timing mmul after multiply: " + (endTime2-startTime))
		
		
		retMatrix
	}
	
	/*
	 * compute: dense matrix with CRS sparse matrix (this.crs)
	 * time complexity is similar "upper bound" by denseMatrix.multAdd api 
	 */
	def mmul2(dmatrix: DoubleMatrix) : DenseMatrix = {
		
		val startTime = System.currentTimeMillis()
		
		
		//converted to dense matrix
		val convertedMatrix = new DenseMatrix(dmatrix.getRows(), dmatrix.getColumns())
		var rows = 0
		var columns = 0
		while(rows < dmatrix.getRows()) {
			columns = 0
			while(columns < dmatrix.getColumns()) {
				convertedMatrix.set(rows, columns, dmatrix.get(rows, columns))
				columns += 1
			}
			rows += 1
		}
		
		
		val endTime = System.currentTimeMillis()
		println(">>>>>>>>>>>>>>>>>> timing mmul2 after convert: " + (endTime-startTime))
		
		//returned matrix
		//m * n  multiply n * p return m * p
		val retMatrix = new DenseMatrix(convertedMatrix.numRows, crs.numColumns())
		
		//TO DO: multAdd is VERY slow 
		println(">>>>>>>>> multiplying matrix: crs: numrwos=" + crs.numRows() + "\t numColumns=" + crs.numColumns() + "\tconvertedMatrix numrows=" + convertedMatrix.numRows() + "\t numCOlumns=" + convertedMatrix.numColumns() )
		convertedMatrix.multAdd(crs, retMatrix)
		
		
		val endTime2 = System.currentTimeMillis()
		println(">>>>>>>>>>>>>>>>>> timing mmul2 after multiadd: " + (endTime2-endTime))
		
		retMatrix
	}
	
	
//	def toVector() : Vector = {
//		null
//	}
//	
//	def transpose(): MatrixSparse = {
//		null
//	}
//	
//	def dot(other: MatrixSparse): Double = {
//		0.0
//	}
//	
//	def neg() : MatrixSparse = {
//		this
//	}
//	
//	def addi(first: MatrixSparse) : MatrixSparse = {
//		this
//	}
//	
//	def length(): Int = {
//		0
//	}
//	
//	
//	def get(i: Int) : Double = {
//		0.0
//	}
}

object MatrixSparse {
	
	/*
	 * return column vecotr
	 */
	def toDoubleMatrix(inputVector: no.uib.cipr.matrix.Vector) : DoubleMatrix = {
		val retMatrix = new DoubleMatrix (inputVector.size())
		var i = 0
		while(i < inputVector.size()) {
			retMatrix.put(i, 0, inputVector.get(i))
			i += 1
		}
		retMatrix
	}
	
	def toDoubleMatrix(inputMatrix: no.uib.cipr.matrix.Matrix) : DoubleMatrix = {
		val retMatrix = new DoubleMatrix (inputMatrix.numRows(), inputMatrix.numColumns())
		var rows = 0
		var columns  = 0
		while(rows < inputMatrix.numRows()) {
			columns  = 0
			while(columns < inputMatrix.numColumns()) {
				retMatrix.put(rows, columns, inputMatrix.get(rows, columns))		
				columns += 1
			}
			rows += 1
		}
		retMatrix
	}
	
//	def mmul(db :DoubleMatrix, X: MatrixSparse) : Vector = {
//		null
//	}
//	
//	def mmul(db :MatrixSparse, X: MatrixSparse) : Vector = {
//		null
//	}
//	
//	def sub(first: MatrixSparse, second: MatrixSparse) : MatrixSparse = {
//		null
//	}
//	
//	def subi(first: MatrixSparse, second: MatrixSparse) : MatrixSparse = {
//		null
//	}
//	
//	def toMatrixSparse(v: Vector) : MatrixSparse = {
//		null
//	}
}