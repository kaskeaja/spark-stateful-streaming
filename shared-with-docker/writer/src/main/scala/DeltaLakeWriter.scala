package DeltaLakeSource

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

import java.sql.Timestamp

import org.apache.spark.sql.functions.{
  explode, udf, split, col,
  element_at, to_timestamp,
  substring, window, max, count}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.UserDefinedFunction

object DeltaLakeWriter {
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .appName("Simple Application")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("--------------------")  // scalastyle:ignore regex
    println("- Application logs -")  // scalastyle:ignore regex
    println("--------------------")  // scalastyle:ignore regex

    def addId(x: Int): String = {
      val rnd = new scala.util.Random

      if (rnd.nextInt(2) > 0) {
        "A"
      } else {
        "B"
      }
    }
    val addIdUDF = udf[String, Int](addId)


    def setBreakpoint(x: Int, timestamp: Timestamp): Option[Float] = {
      val rnd = new scala.util.Random
      val periodicity = 240

      // 50% change to skip value and deterministic intervals of missing values
      if (rnd.nextInt(2) > 0 || timestamp.getTime % (periodicity*1000) < 35000 ) {
        None
      } else {
        Some((x % periodicity).toFloat)
      }
    }
    val setBreakpointUDF = udf[Option[Float], Int, Timestamp](setBreakpoint)


    val rowsPerSecond = 3
    val inputStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", rowsPerSecond)
      .load()

    val processedStream = inputStream
      .withColumn("id", addIdUDF(col("value")))
      .withColumn("data", setBreakpointUDF(col("value"), col("timestamp")))
      .withColumnRenamed("timestamp", "eventTime")
      .withWatermark("eventTime", "5 seconds")
      .select("id", "data", "eventTime")

    processedStream.printSchema

    val deltalakeID = '1'
    val stream = processedStream
      .writeStream
      .format("delta")
      .trigger(Trigger.ProcessingTime("200 milliseconds"))
      .outputMode("append")
      .option("checkpointLocation", s"/tmp/checkpoint-$deltalakeID")
      .start(s"/home/developer/shared_with_host/delta-lake-storage/delta-table-$deltalakeID")
      .awaitTermination()
  }
}
