package com.spark.struct

object SparkStructKafkaApp extends InitSpark {

  def main(args: Array[String]) = {

    import spark.implicits._

    val version = spark.version
    println("SPARK VERSION = " + version)

    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> "scdf-release-kafka-headless:9092",
      "key.deserializer" -> " org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> " org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "structured-kafka","startingOffsets"-> "earliest" , "maxOffsetsPerTrigger" -> "20" ,
      "failOnDataLoss" -> "false","kafka.partition.assignment.strategy" -> "range")

    val data = spark.
      readStream.
      format("kafka").
      option("subscribe", "springcloud-stream-kafka-mesages").
      options(kafkaParams).
      load().
      selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").
      as[(String, String)]

    data.printSchema()

    data.writeStream.
      outputMode("append").
        format("console").
          start().
      awaitTermination()
  }
}
