/**
 *
 */
package adatao.spark

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import adatao.ML.ATestSuite
import adatao.ML.spark.SharedSparkContext
import adatao.spark.RDDImplicits._

/**
 * @author ctn
 *
 */
@RunWith(classOf[JUnitRunner])
class RDDImplicitsSuite extends ATestSuite with SharedSparkContext {
	lazy val emptyRdd = sc.parallelize(List[Int]())
	lazy val singleDigitRdd = sc.parallelize(0 to 9, 10)

	test("OK to map() on empty collections") {
		assert(emptyRdd.map(x ⇒ x).count === 0)
	}

	test("Not ok to reduce() on empty collections") {
		intercept[UnsupportedOperationException] {
			emptyRdd.map(x ⇒ x).reduce(_ + _)
		}

		intercept[UnsupportedOperationException] {
			singleDigitRdd.filter(x ⇒ x > 50).reduce(_ + _)
		}
	}

	test("safeReduce() works") {
		assert(singleDigitRdd.map(x ⇒ x).safeReduce(_ + _) === 45)
		assert(singleDigitRdd.map(x ⇒ x).safeReduce((_ + _), 31415927) === 45)
	}

	test("OK to safeReduce() on empty collections") {
		assert(emptyRdd.map(x ⇒ x).safeReduce(_ + _) === 0)
		assert(emptyRdd.map(x ⇒ x).safeReduce((_ + _), 31415927) === 31415927)

		assert(singleDigitRdd.filter(x ⇒ x > 50).safeReduce(_ + _) === 0)
		assert(singleDigitRdd.filter(x ⇒ x > 50).safeReduce((_ + _), 31415927) === 31415927)
	}

	test("Not OK to first() on empty collections") {
		intercept[UnsupportedOperationException] {
			emptyRdd.map(x ⇒ x).first()
		}

		intercept[UnsupportedOperationException] {
			singleDigitRdd.filter(x ⇒ x > 50).first()
		}
	}

	test("OK to safeFirst() on empty collections") {
		assert(emptyRdd.map(x ⇒ x).safeFirst() === 0)
		assert(emptyRdd.map(x ⇒ x).safeFirst(31415927) === 31415927)

		assert(singleDigitRdd.filter(x ⇒ x > 50).safeFirst() === 0)
		assert(singleDigitRdd.filter(x ⇒ x > 50).safeFirst(31415927) === 31415927)
	}

}
