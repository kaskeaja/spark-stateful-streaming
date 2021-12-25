package deltaLakeProcessor

import java.sql.Timestamp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.apache.spark.sql.types.TimestampType


import org.apache.spark.sql.functions.{
  explode, udf, split, col,
  element_at, to_timestamp,
  substring, window, max, count}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{
  Trigger,
  GroupState,
  GroupStateTimeout,
  OutputMode
}

case class InputStreamType(id: String, data: Float, eventTime: Timestamp)
case class OutputStreamData(id: String, value: Double, windowStart: Timestamp, windowSize: Long, info: String)  // info for debug purposes

case class GroupedEvent(data: Float, eventTime: Timestamp)

case class RunningSession(
  dataBuffer: List[deltaLakeProcessor.Datapoint],
  lastBeforeBuffer: Option[deltaLakeProcessor.Datapoint],  // Doesn't exist in the first invokation but once set always exists
  startEpochOfOnePastLastProcessedWindow: Long,
  callId: Int  // For debug purposes
  )

object DeltaLakeProcessor {
  def main(args: Array[String]) {  // scalastyle:ignore cyclomatic.complexity method.length
    val spark = SparkSession
        .builder
        .appName("Simple Application")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    println("--------------------")  // scalastyle:ignore regex
    println("- Application logs -")  // scalastyle:ignore regex
    println("--------------------")  // scalastyle:ignore regex

    def lastOrNone(data: List[deltaLakeProcessor.Datapoint]): Option[deltaLakeProcessor.Datapoint] = {
      if (data.length > 0) {
        Some(data.maxBy(_.timestamp.getTime))
      } else {
        None
      }
    }

    def window2OutputStreamData(id: String, windowSize: Long, input: deltaLakeProcessor.Window, info: String): OutputStreamData = {
      new OutputStreamData(id, input.value, input.windowstart, windowSize, info)
    }

    // See:
    //  https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/GroupState.html
    //  https://blog.yuvalitzchakov.com/exploring-stateful-streaming-with-spark-structured-streaming/

    // Three different paths:
    // 1. Timeout path. Invoked with no data due to timeout
    // 2. Invoked due to data path. 'Standard path', invoked if system has new data and has been invoked before
    // 3. First invokation path.
    //
    // Concepts and ideas:
    // Watermark:
    //    Spark strucuted streaming concept limiting what data function receives
    // startEpochOfOnePastLastProcessedWindow:
    //    Window that is not part of the output. Note that invokation 'startEpochOfOnePastLastProcessedWindow'
    //    in step n is the start window of output in step n + 1
    //
    // Last Datapoint of previous output is always added to the data going into breakpoint transformation
    // so that there are no timepoints with indetermined values.
    //
    def stateProcessor(
      id: String,
      inputs: Iterator[InputStreamType],
      state: GroupState[RunningSession]
      ): Iterator[OutputStreamData] = {

      val prefixedPrintln = if (state.exists) {
        val callId = state.get.callId
        (message: String) => println(s"callId ${callId}|ID $id: $message")  // scalastyle:ignore regex
      } else {
        (message: String) => println(s"ID $id: $message")  // scalastyle:ignore regex
      }

      val groupedEvent = inputs.toList.map(x => new GroupedEvent(x.data, x.eventTime))
      val windowSize = 1000

      val assumedStartEpochOfOnePastLastProcessedWindow = deltaLakeProcessor.epoch2WindowEpoch(state.getCurrentWatermarkMs, windowSize) + windowSize

      val timeoutDescription = "10 second"

      if (state.hasTimedOut) {
        // Timeout path:
        //
        // If buffer has data, then process the data and write to output up to startEpochOfOnePastLastProcessedWindow
        // Otherwise return empty iterator
        //

        // Just to make sure that there is no new data
        assert(groupedEvent.length == 0)

        val existingState = state.get
        state.remove()

        // To make sure that startEpochOfOnePastLastProcessedWindow doesn't go backwards.
        // This is 'startEpochOfOnePastLastProcessedWindow' of this invokation.
        // That is, no data is written to output with timestamp in this window.
        val startEpochOfOnePastLastProcessedWindow = if (assumedStartEpochOfOnePastLastProcessedWindow < existingState.startEpochOfOnePastLastProcessedWindow) {
          prefixedPrintln("Watermark based startEpochOfOnePastLastProcessedWindow tried to go backwards")
          existingState.startEpochOfOnePastLastProcessedWindow
        } else {
          assumedStartEpochOfOnePastLastProcessedWindow
        }

        val (outputState, remainingState) = existingState.dataBuffer.partition(_.timestamp.getTime < startEpochOfOnePastLastProcessedWindow)

        // Guarantees that if lastBeforeBuffer is set in previous call, then it is set also in this call
        val fullOutput: List[deltaLakeProcessor.Datapoint] = existingState.lastBeforeBuffer match {
          case Some(lastBeforeBuffer) => List.concat(outputState, List(lastBeforeBuffer))
          case None => outputState
        }

        // Sets next invokation state
        state.update(new RunningSession(remainingState, lastOrNone(fullOutput), startEpochOfOnePastLastProcessedWindow, existingState.callId + 1))
        state.setTimeoutDuration(timeoutDescription)

        // Just to make sure there is no bug in logic above
        if (existingState.lastBeforeBuffer != None) {
          assert(fullOutput.length > 0)
          assert(lastOrNone(fullOutput) != None)
        }

        if (fullOutput.length > 0) {
          val startEpoch = existingState.startEpochOfOnePastLastProcessedWindow
          val endEpoch = startEpochOfOnePastLastProcessedWindow - 1  // minus -1 needed so that no extra window is generated

          if (startEpoch >= endEpoch) {
            prefixedPrintln(s"Empty output from 'Timeout path' because 'endEpoch' ${new Timestamp(endEpoch)} is before 'startEpoch' ${new Timestamp(startEpoch)}.")  // scalastyle:ignore line.size.limit
          }
          val info = s"${existingState.callId} timeout path ${new Timestamp(startEpoch)} -> ${new Timestamp(endEpoch)}"

          deltaLakeProcessor
            .evaluateDatapoints(fullOutput, startEpoch, endEpoch, windowSize)
            .iterator.map(x => window2OutputStreamData(id, windowSize, x, info))
        } else {
          prefixedPrintln("'Timeout path' empty output")
          Iterator[OutputStreamData]()
        }
      } else if (state.exists) {
        //
        // Case: State with existing data invoked due to new data and trigger
        //

        val existingState = state.get

        // To make sure that startEpochOfOnePastLastProcessedWindow doesn't go backwards.
        // This is 'startEpochOfOnePastLastProcessedWindow' of this invokation.
        // That is, no data is written to output with timestamp in this window.
        val startEpochOfOnePastLastProcessedWindow = if (assumedStartEpochOfOnePastLastProcessedWindow < existingState.startEpochOfOnePastLastProcessedWindow) {
          prefixedPrintln("Watermark based startEpochOfOnePastLastProcessedWindow tried to go backwards")
          existingState.startEpochOfOnePastLastProcessedWindow
        } else {
          assumedStartEpochOfOnePastLastProcessedWindow
        }

        // The state exists so let's first filter out all data that has already been
        // processed or that is just too late
        val (outputAddition, stateAddition) = existingState.lastBeforeBuffer match {
          case Some(lastBeforeBuffer) => {
            groupedEvent
              .filter(_.eventTime.getTime >= lastBeforeBuffer.timestamp.getTime)  // Spark doesn't guarantee that late data cannot show up here
              .map(x => new deltaLakeProcessor.Datapoint(x.eventTime, x.data))
              .partition(_.timestamp.getTime < startEpochOfOnePastLastProcessedWindow)
          };
          case None => {
            groupedEvent
              .map(x => new deltaLakeProcessor.Datapoint(x.eventTime, x.data))
              .partition(_.timestamp.getTime < startEpochOfOnePastLastProcessedWindow)
          };
        }

        val (outputState, remainingState) = state.get.dataBuffer.partition(_.timestamp.getTime < startEpochOfOnePastLastProcessedWindow)
        state.remove()

        // Guarantees that if lastBeforeBuffer is set in previous call, then it is set also in this call
        val fullOutput = existingState.lastBeforeBuffer match {
          case Some(lastBeforeBuffer) => List.concat(outputState, outputAddition, List(lastBeforeBuffer));
          case None => List.concat(outputState, outputAddition);
        }

        val newStateData = List.concat(remainingState, stateAddition)
        state.update(new RunningSession(newStateData, lastOrNone(fullOutput), startEpochOfOnePastLastProcessedWindow, existingState.callId + 1))
        state.setTimeoutDuration(timeoutDescription)

        if (existingState.lastBeforeBuffer != None) {
          assert(fullOutput.length > 0)
          assert(lastOrNone(fullOutput) != None)
        }

        if (fullOutput.length > 0) {
          val startEpoch = existingState.startEpochOfOnePastLastProcessedWindow
          val endEpoch = startEpochOfOnePastLastProcessedWindow - 1  // minus -1 needed so that no extra window is generated

          if (startEpoch >= endEpoch) {
            prefixedPrintln(s"Empty output from 'Invoked due to data path' because 'endEpoch' ${new Timestamp(endEpoch)} is before 'startEpoch' ${new Timestamp(startEpoch)}.")  // scalastyle:ignore line.size.limit
          } else {
            prefixedPrintln("'Invoked due to data path' output with data")
          }

          val info = s"${existingState.callId} invokated due to data path ${new Timestamp(startEpoch)} -> ${new Timestamp(endEpoch)}"

          deltaLakeProcessor
            .evaluateDatapoints(fullOutput, startEpoch, endEpoch, windowSize)
            .iterator.map(x => window2OutputStreamData(id, windowSize, x, info))
        } else {
          prefixedPrintln("'Invoked due to data path' empty output")
          // This could happen if 'lastBeforeBuffer' is still None
          // and this path is invoked data that as eventTime newer
          // than 'startEpochOfOnePastLastProcessedWindow'
          Iterator[OutputStreamData]()
        }
      } else {
        //
        // Case: first invokation of this function, "Batch path" so that next timeout will output state
        //
        prefixedPrintln("First invokation path")

        val startEpochOfOnePastLastProcessedWindow = assumedStartEpochOfOnePastLastProcessedWindow

        val mappedEvents = groupedEvent
          .map(x => new deltaLakeProcessor.Datapoint(x.eventTime, x.data))

        // Note that the first watermark is always zero and therefore all of the data goes to stateAddition
        val (outputAddition, stateAddition) = mappedEvents
          .partition(_.timestamp.getTime < startEpochOfOnePastLastProcessedWindow)

        val updatedSession = new RunningSession(
          stateAddition,  // An empty list in this case
          // Usually there is no data in outputAddition in this path and therefore lastBeforeBuffer is None
          lastOrNone(outputAddition),
          // Let's set the processed data watermark to beginning of received data
          // since no data is written to output in this path
          stateAddition.minBy(_.timestamp.getTime).timestamp.getTime,
          0
        )

        state.update(updatedSession)
        state.setTimeoutDuration(timeoutDescription)

        Iterator[OutputStreamData]()
      }
    }

    val deltalakeID = '1'
    val inputStream = spark
      .readStream.format("delta")
      .load(s"/home/developer/shared_with_host/delta-lake-storage/delta-table-$deltalakeID")
      .filter(col("data").isNotNull)

    inputStream.printSchema

    val processedStream = inputStream
      .map(x => new InputStreamType(x.getString(0), x.getFloat(1), x.getTimestamp(2)))
      .withWatermark("eventTime", "1 minute")
      .groupByKey(_.id)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.ProcessingTimeTimeout)(stateProcessor)

    val stream = processedStream
      .writeStream
      .format("delta")
      .trigger(Trigger.ProcessingTime("200 milliseconds"))
      .outputMode("append")
      .option("checkpointLocation", s"/tmp/checkpoint-$deltalakeID")
      .start(s"/home/developer/shared_with_host/delta-lake-storage/delta-table-processed-$deltalakeID")
      .awaitTermination()
  }
}
