# spark-etl

[![Build Status](https://travis-ci.org/vngrs/spark-etl.svg?branch=master)](https://travis-ci.org/vngrs/spark-etl)
[![Coverage Status](https://coveralls.io/repos/github/vngrs/spark-etl/badge.svg?branch=master)](https://coveralls.io/github/vngrs/spark-etl?branch=master)
[![Join the chat at https://gitter.im/vngrs/spark-etl](https://badges.gitter.im/vngrs/spark-etl.svg)](https://gitter.im/vngrs/spark-etl?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](http://badges.mit-license.org)

### What is spark-etl?

[The ETL(Extract-Transform-Load)] process is a key component of many data management operations, including move data and to transform the data from one format to another. To effectively support these operations, spark-etl is providing a distributed solution.

spark-etl is a Scala-based project and it is developing with Spark. So it is scalable and distributed. spark-etl will process data from N source to N database.
The project structure:
- Extract
  - FILES (json, csv)
  - SQLs
  - NoSQLs
  - Key Value Stores
  - APIs
  - Streams

- Transform
  - Row to json
  - Row to csv
  - Json to Row
  - Change records
  - Merge records

- Load
  - FILES (json, csv)
  - SQLs
  - NoSQLs
  - Key Value Stores
  - APIs
  - Streams

Differences between other ETL projects:
  - parallel ETL on cluster level
  - synchronisation of data
  - open source

### Example Scenario

We want to get data from multiple sources like MySQL and CVS. When we extracting data, we also want to filter and merge some fields/tables. During the transform layer, we want to run an SQL. Then we want to write the transformed data to multiple targets like S3 and Redshift.

![etl](http://goo.gl/nZMmIE)

spark-etl is the easiest way to do this scenario!


### Tech
* [Scala] - Functional Programming Language
* [ScalaTest] - ScalaTest is a testing tool in the Scala ecosystem.
* [wartremover] - WartRemover is a flexible Scala code linting tool.
* [scalastyle] - Scalastyle examines Scala code and indicates potential problems with it.
* [scoverage] - Scoverage is a code coverage tool for scala that offers statement and branch coverage.
* [Apache Spark] - Apache Spark is a fast and general engine for large-scale data processing.
* [travis-ci] - Travis CI is a hosted, distributed continuous integration service used to build and test software projects
* [coveralls] - Coveralls is a web service to help you track your code coverage over time, and ensure that all your new code is fully covered


### Installation

Prerequisites for building spark-etl:

- sbt clean assembly

### How to become a committer

Want to contribute? Great!
Let's say "Hello" on [gitter].


### Todos

 - Scalafmt integration
 - ETL Design

License
----
[MIT License]

   [etl]: <https://github.com/vngrs/spark-etl>
   [The ETL(Extract-Transform-Load)]: <https://en.wikipedia.org/wiki/Extract,_transform,_load>
   [gitter]: https://gitter.im/vngrs/spark-etl?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
   [MIT License]: https://github.com/vngrs/spark-etl/blob/master/LICENSE
   [Scala]: https://github.com/scala/scala
   [ScalaTest]: https://github.com/scalatest/scalatest
   [wartremover]: https://github.com/puffnfresh/wartremover
   [scalastyle]: https://github.com/scalastyle/scalastyle
   [travis-ci]: https://github.com/travis-ci/travis-ci
   [coveralls]: https://coveralls.io/
   [Apache Spark]: https://github.com/apache/spark