package com.app

import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import org.apache.hadoop.mrunit.TestDriver
import org.apache.hadoop.conf.Configuration

trait HadoopTestCase[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends FunSpec with ShouldMatchers with HadoopImplicitConversions with BeforeAndAfter {
  val driver: TestDriver[KEYIN, VALUEIN, KEYOUT, VALUEOUT]

  before {
    driver.setConfiguration(new Configuration(false))
    driver.getConfiguration.setQuietMode(false)
    setupDriver()
  }

  def setupDriver()

  after {
    driver.resetOutput()
  }
}
