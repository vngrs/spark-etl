package com.vngrs.etl.exceptions

/**
  * Represents a Schema expected but none is found.
  *
  * @param msg The detailed message saved for later to be retrieved by `getMessage` method.
  */
class NoSchemaException(msg: String) extends RuntimeException(msg)
