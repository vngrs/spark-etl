package com.vngrs.etl

/**
  * A Hello World application test specs
  */
class ApplicationSpec extends BaseSpec {

  "A Hello World application" should "print Hello WORLD!" in {
    val stream = new java.io.ByteArrayOutputStream()

    Console.withOut(stream) {
      Application.main(Array())
    }

    stream.toString("UTF8") should be ("Hello WORLD!")
  }

}
