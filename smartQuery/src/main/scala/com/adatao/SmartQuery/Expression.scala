package com.adatao.SmartQuery

/**
 * author: daoduchuan
 */
abstract sealed class Expression

case class Filtering(left: String, right: String, comparison: String) extends Expression
