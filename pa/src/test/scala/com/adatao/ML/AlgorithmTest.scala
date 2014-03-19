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

package com.adatao.ML

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.scalatest.BeforeAndAfterEach

/**
 * @author ctn
 *
 */
trait TTestSuite extends FunSuite with BeforeAndAfterEach {
	private lazy val testNameArray: Array[String] = testNames.toArray
	private var testNumber: Int = 0
	def getCurrentTestName = "Test #%d: %s".format(testNumber + 1, testNameArray(testNumber))

	override def afterEach = {
		testNumber += 1
	}

	def truncate(x: Double, n: Int) = {
		def p10(n: Int, pow: Long = 10): Long = if (n == 0) pow else p10(n - 1, pow * 10)
		if (n < 0) {
			val m = p10(-n).toDouble
			math.round(x / m) * m
		}
		else {
			val m = p10(n - 1).toDouble
			math.round(x * m) / m
		}
	}
}

trait TTestCanLog {
	val LOG: Logger = LoggerFactory.getLogger(this.getClass())
}

@RunWith(classOf[JUnitRunner])
abstract class ATestSuite extends TTestSuite with TTestCanLog{

	override def beforeEach = {
		LOG.info("%s started".format(this.getCurrentTestName))
	}

	override def afterEach = {
		LOG.info("%s ended".format(this.getCurrentTestName))
		super.afterEach
	}
}

abstract class AAlgorithmTest extends ATestSuite {
}

abstract class ATimedAlgorithmTest extends AAlgorithmTest {
	private var startTime: Long = 0

	override def beforeEach = {
		LOG.info("%s started".format(this.getCurrentTestName))
		startTime = System.nanoTime
	}

	override def afterEach = {
		val stopTime = System.nanoTime
		val ms = (stopTime - startTime) / 1000000
		LOG.info("%s ended, %d ms elapsed".format(this.getCurrentTestName, ms))

		super.afterEach
	}
}
