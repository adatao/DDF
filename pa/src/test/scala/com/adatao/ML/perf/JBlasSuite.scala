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

package com.adatao.ML.perf

import org.scalatest.Engine
import com.adatao.ML.ATimedAlgorithmTest
import org.jblas.DoubleMatrix
import scala.util.Random
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.jblas.SimpleBlas
import org.jblas.NativeBlas

@RunWith(classOf[JUnitRunner])
class JBlasSuite extends ATimedAlgorithmTest {
	val doTest = false
	val testOrIgnore = if (doTest) test _ else ignore _

	val M = 100000
	val N = 100
	val REPS = 100

	val a = DoubleMatrix.rand(M, N)
	val b = DoubleMatrix.rand(N, M)
	val x = DoubleMatrix.rand(N, 1)
	val y = DoubleMatrix.zeros(M, 1)

	val K = 100
	val x2 = DoubleMatrix.rand(N, K)
	val y2 = DoubleMatrix.rand(M, K)

	val alpha = 1.0
	val beta = 0.0

	testOrIgnore("Automatic Java Matrix-Vector [%dx%d]x[%dx%d] multiplication, %d reps".format(M, N, N, 1, REPS)) {
		(1 to REPS).foreach({ i ⇒ a.mmul(x) })
	}

	testOrIgnore("BLAS Level 2: SimpleBlas.gemv Matrix-Vector [%dx%d]x[%dx%d] multiplication, %d reps".format(M, N, N, 1, REPS)) {
		(1 to REPS).foreach({ i ⇒ SimpleBlas.gemv(alpha, a, x, beta, y) })
	}

	testOrIgnore("BLAS Level 2: NativeBlas.dgemv Matrix-Vector [%dx%d]x[%dx%d] multiplication, %d reps".format(M, N, N, 1, REPS)) {
		(1 to REPS).foreach({ i ⇒ NativeBlas.dgemv('N', a.rows, a.columns, alpha, a.data, 0, a.rows, x.data, 0, 1, beta, y.data, 0, 1) })
	}

	testOrIgnore("BLAS Level 3: NativeBlas.dgemm Matrix-Vector [%dx%d]x[%dx%d] multiplication, %d reps".format(M, N, N, 1, REPS)) {
		val c = y
		(1 to REPS).foreach({ i ⇒ NativeBlas.dgemm('N', 'N', c.rows, c.columns, a.columns, alpha, a.data, 0, a.rows, b.data, 0, b.rows, beta, c.data, 0, c.rows) })
	}

	testOrIgnore("Automatic Java Matrix-Matrix [%dx%d]x[%dx%d] multiplication, %d reps".format(M, N, N, K, REPS)) {
	  val x = x2
		val y = y2
		(1 to REPS).foreach({ i ⇒ a.mmul(x) })
	}

	testOrIgnore("BLAS Level 3: NativeBlas.dgemm Matrix-Matrix [%dx%d]x[%dx%d] multiplication, %d reps".format(M, N, N, K, REPS)) {
	  val x = x2
		val y = y2
		val c = y
		(1 to REPS).foreach({ i ⇒ NativeBlas.dgemm('N', 'N', c.rows, c.columns, a.columns, alpha, a.data, 0, a.rows, b.data, 0, b.rows, beta, c.data, 0, c.rows) })
	}
}
