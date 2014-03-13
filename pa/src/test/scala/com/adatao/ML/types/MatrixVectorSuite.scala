/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package adatao.ML.types

import adatao.ML.ATestSuite
import adatao.ML.AAlgorithmTest
import adatao.ML.ALossFunction
import org.jblas.MatrixFunctions
import org.jblas.DoubleMatrix
import org.junit.Assert.assertEquals
import java.util.HashMap
import com.adatao.pa.spark.execution.TransformSparseMatrix


class MatrixVectorSuite extends ATestSuite {

//	test("Vector instantiation with Array[Double]") {
//		val array = Array(1.0, 2.0, 3.0)
//		val vector = Vector(array)
//		assert(vector != null)
//		assert(vector.rows == array.size)
//
//		var i = 0
//		while (i < array.size) {
//			assert(vector(i) === array(i))
//			i += 1
//		}
//	}
//
//	test("Can compute sigmoid of extremely large z's") {
//		val bigz = Vector(1e4, -1e4, 1e6, -1e6)
//		val sigmoid = ALossFunction.sigmoid(bigz)
//		assert(sigmoid === Vector(1.0, 0.0, 1.0, 0.0))
//	}
//	
//	test("Can compute Xtx ") {
//		val array = Array(1.0, 2.0, 3.0, 1.0, 2.0, 3.0)
//		//matrix 2*3
//		val m = new DoubleMatrix(2, 3, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0)
//		
//		val x = Matrix(m)
//		val x2 = x.transpose()
//		val xtx2 = x2.mmul(x)
//		val xtx = x.XtX
//		
//		println(">>>>>>>>>>>>>>>>>>>>>>> new method" + xtx)
//		println(">>>>>>>>>>>>>>>>>>>>>>> old method" + xtx2)
//		
//		//xtx must be identical with xtx2
//		assertEquals(xtx.get(2,2), xtx2.get(2,2), 0.00001)
//		
//	}
//	
//	test("Can compute loss with sigmoid of extremely large z's") {
//		val bigz = Vector(1e4, -1e4, 1e6, -1e6)
//		val hypothesis = ALossFunction.sigmoid(bigz)
//		//		println(">>>> hypothesis:" + hypothesis)
//		val Y = Vector(1, 1, 1, 1)
//		val YT = Y.transpose()
//
//		val logH = ALossFunction.safeLogOfSigmoid(hypothesis, bigz)
//		val log1minusH = ALossFunction.safeLogOfSigmoid(Vector.fill(Y.length, 1.0).subi(hypothesis), bigz.neg)
//
//		assert(logH === Vector(0, -1e4, 0, -1e6))
//		assert(log1minusH === Vector(-1e4, 0, -1e6, 0))
//
//		val lossA: Double = Y.dot(logH) // Y' x log(h)
//		val lossB: Double = Vector.fill(Y.length, 1.0).subi(Y).dot(log1minusH) // (1-Y') x log(1-h)
//
//		assert(lossA === -(1e6 + 1e4))
//		assert(lossB === 0.0)
//	}
	
//	test("Can transform to sparse matrix ") {
//		//matrix 2*3
//		val m = new DoubleMatrix(2, 3, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0)
//		val x = Matrix(m)
//		val y = Vector(1, 1)
//		
//		var sparseColumns = new HashMap [Int, Boolean]()
//		sparseColumns.put(1, true)
//		
//		val maxCellValue = 3
//		
//		val transform = new TransformSparseMatrix(sparseColumns, maxCellValue);
//		val (newm, newv) = transform.transformSparse(sparseColumns)(x, y)
//		
//		println(">>>>>>>>>>>>>>>>>>>>>>> old matrix " + x)
//		println(">>>>>>>>>>>>>>>>>>>>>>> new sparse matrix " + newm.crs)
//		
//	}

//	test("Can mmul sparse matrix with vector") {
//		val m = new DoubleMatrix(2, 3, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0)
//		val x = Matrix(m)
//		val y = Vector(1, 1)
//		val z = Vector(1, 3, 4, 5, 6, 7)
//		val z2 = Vector(1, 3, 4)
//		
//		var sparseColumns = new HashMap [Int, Boolean]()
////		sparseColumns.put(1, true)
//		
//		val maxCellValue = 3
//		
//		val transform = new TransformSparseMatrix(sparseColumns, maxCellValue);
//		val (newm, newv) = transform.transformSparse(sparseColumns)(x, y)
//		
//		println(">>>>>>>>>>>>>>>>>>>>>>> old matrix " + x)
//		println(">>>>>>>>>>>>>>>>>>>>>>> new sparse matrix ")
//		newm.print
//		
//		val result = newm.mmul(z2)
//		
//		println(">>result matrix: " + result)
////		assertEquals(result.get(0), 16.000000, 0.000000)
////		assertEquals(result.get(1), 19.000000, 0.000000)
//		
//	}
////	
//	test("Can mmul sparse matrix with matrix") {
//		val m = new DoubleMatrix(2, 3, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0)
//		val a = new DoubleMatrix(6, 3, 1.0, 3.0, 2.0, 2.0, 4.0, 6.0, 3.0, 4.0, 5.0, 6.0, 2.0, 1.0, 3.0, 4.0, 7.0, 1.0, 2.0, 3.0)
//		
//		val x = Matrix(m)
//		val y = Vector(1, 1)
//		
//		var sparseColumns = new HashMap [Int, Boolean]()
//		sparseColumns.put(1, true)
//		
//		//TO DO check this later :(
//		val maxCellValue = 3
//		
//		val transform = new TransformSparseMatrix(sparseColumns, maxCellValue);
//		val (newm, newv) = transform.transformSparse(sparseColumns)(x, y)
//		
//		println(">>>>>>>>>>>>>>>>>>>>>>> old matrix " + x)
//		println(">>>>>>>>>>>>>>>>>>>>>>> new sparse matrix ")
//		newm.print
//		
//		println("matrix a = " + a)
//		
//		val result = newm.mmul(a)
//		
//		assertEquals(result.get(0, 0), 11.000000, 0.000000)
//		assertEquals(result.get(0, 1), 14.000000, 0.000000)
//		assertEquals(result.get(0, 2), 20.000000, 0.000000)
//		assertEquals(result.get(1, 0), 10.000000, 0.000000)
//		assertEquals(result.get(1, 1), 27.000000, 0.000000)
//		assertEquals(result.get(1, 2), 28.000000, 0.000000)
//		
//	}
//	
//	test("Can mmul sparse matrix with matrix no sparse column") {
//		val m = new DoubleMatrix(2, 6, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 4.0, 2.0, 2.0, 1.0, 3.0, 3.0)
//		val a = new DoubleMatrix(6, 3, 1.0, 3.0, 2.0, 2.0, 4.0, 6.0, 3.0, 4.0, 5.0, 6.0, 2.0, 1.0, 3.0, 4.0, 7.0, 1.0, 2.0, 3.0)
//		
//		val x = Matrix(m)
//		val y = Vector(1, 1)
//		
//		var sparseColumns = new HashMap [Int, Boolean]()
////		sparseColumns.put(1, true)
//		
//		//TO DO check this later :(
//		val maxCellValue = 3
//		
//		val transform = new TransformSparseMatrix(sparseColumns, maxCellValue);
//		val (newm, newv) = transform.transformSparse(sparseColumns)(x, y)
//		
//		println(">>>>>>>>>>>>>>>>>>>>>>> old matrix " + x)
//		println(">>>>>>>>>>>>>>>>>>>>>>> new sparse matrix ")
//		newm.print
//		
//		println("matrix a = " + a)
//		
//		val result = newm.mmul(a)
//		
////		assertEquals(result.get(0, 0), 11.000000, 0.000000)
////		assertEquals(result.get(0, 1), 14.000000, 0.000000)
////		assertEquals(result.get(0, 2), 20.000000, 0.000000)
////		assertEquals(result.get(1, 0), 10.000000, 0.000000)
////		assertEquals(result.get(1, 1), 27.000000, 0.000000)
////		assertEquals(result.get(1, 2), 28.000000, 0.000000)
//		
//	}
	
}

