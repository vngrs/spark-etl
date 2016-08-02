package com.vngrs.etl

import org.scalatest._

/**
  * Base Test Specification
  */
abstract class BaseSpec extends FlatSpec
  with Matchers with OptionValues with Inspectors
