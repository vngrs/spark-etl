package com.vngrs.etl.codecs

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import com.vngrs.etl.BaseSpec
import com.vngrs.etl.configs.ToCsvCodecConfigs
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
  * [[org.apache.spark.sql.Row]] to `CSV` convertible format codec
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class RowToCsvCodecSpec extends BaseSpec {

  "A Row To CSV codec" should "convert Row to CSV convertible format" in {
    val data = new GenericRow(Array[Any](1L, "John", "Doe", 35L))

    val codec = RowToCsvCodec()
    val conf = ToCsvCodecConfigs(new SimpleDateFormat())

    codec(data, conf) should contain theSameElementsAs Seq[Any](1L, "John", "Doe", 35L)
  }

  it should "convert Timestamp to string" in {
    // to prevent this test to fail in different environments
    val defaultTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.clear()
    cal.set(2000, Calendar.JANUARY, 1, 0, 0)

    val data = new GenericRow(Array[Any](new Timestamp(cal.getTimeInMillis)))

    val codec = RowToCsvCodec()
    val conf = ToCsvCodecConfigs(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX"))

    codec(data, conf) should contain theSameElementsAs Seq[Any]("2000-01-01T00:00:00Z")

    // to prevent this test to fail other tests because of timezone setup
    TimeZone.setDefault(defaultTimeZone)
  }

  it should "convert util.Date to string" in {
    // to prevent this test to fail in different environments
    val defaultTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.clear()
    cal.set(2000, Calendar.JANUARY, 1, 0, 0)

    val data = new GenericRow(Array[Any](cal.getTime))

    val codec = RowToCsvCodec()
    val conf = ToCsvCodecConfigs(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX"))

    codec(data, conf) should contain theSameElementsAs Seq[Any]("2000-01-01T00:00:00Z")

    // to prevent this test to fail other tests because of timezone setup
    TimeZone.setDefault(defaultTimeZone)
  }

  it should "convert sql.Date to string" in {
    // to prevent this test to fail in different environments
    val defaultTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.clear()
    cal.set(2000, Calendar.JANUARY, 1, 0, 0)

    val data = new GenericRow(Array[Any](new java.sql.Date(cal.getTimeInMillis)))

    val codec = RowToCsvCodec()
    val conf = ToCsvCodecConfigs(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX"))

    codec(data, conf) should contain theSameElementsAs Seq[Any]("2000-01-01T00:00:00Z")

    // to prevent this test to fail other tests because of timezone setup
    TimeZone.setDefault(defaultTimeZone)
  }

  it should "handle empty rows" in {
    val data = new GenericRow(Array[Any]())

    val codec = RowToCsvCodec()
    val conf = ToCsvCodecConfigs(new SimpleDateFormat(""))

    codec(data, conf) should contain theSameElementsAs Seq[Any]()
  }
}
