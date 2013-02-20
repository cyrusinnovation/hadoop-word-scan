package com.app

import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import org.apache.hadoop.mrunit.TestDriver

trait HadoopTestCase[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends FunSpec with ShouldMatchers with HadoopImplicitConversions with BeforeAndAfter {
  val mapDriver: TestDriver[KEYIN, VALUEIN, KEYOUT, VALUEOUT]

  before {
    mapDriver.getConfiguration.setQuietMode(false)
    setupDriver()
  }

  def setupDriver()

  after {
    mapDriver.resetOutput()
  }
}
