/**
 *
 */
package com.adatao.DDF.spark

import com.adatao.DDF.IHandleRepresentations
import java.lang.Class

/**
 * @author ctn
 *
 */
class RepresentationHandler extends IHandleRepresentations {
	def getAsRows(arrayType: Class[_], elementType: Class[_]): Object = { null }
	def getAsColumns(arrayType: Class[_], elementType: Class[_]): Object = { null }
	def setAsRows(rows: Object, arrayType: Class[_], elementType: Class[_]) = {}
}