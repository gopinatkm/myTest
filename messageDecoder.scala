package com.message.util
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.{StructType,StructField, StringType}

  object messageDecoder extends SessionInitiator {

  def main (args : Array[String]) : Unit = {
    val log = LoggerFactory.getLogger(getClass)
    val spark = BuildSparkSession("messageDecoder","local")

    val SourceSchema = StructType(Array(
      StructField("id", StringType, true),
      StructField("changeTime", StringType, true),
      StructField("message", StringType, true)))

    val readDF = spark.read.format("csv")
                      .option("header", "false")
                      .option("delimiter", "|")
                      .schema(SourceSchema)
                      .load("s3a://testgopibkt/srcdata/")

    readDF.show(1)

  }

}
