package deltaLakeProcessor

import java.sql.Timestamp

package object deltaLakeProcessor {

  case class Window(windowstart: Timestamp, value: Double)
  case class IntervalWindow(windowstart: Timestamp, timestamp: Timestamp, value: Option[Double]=None)
  case class Datapoint(timestamp: Timestamp, value: Double)

  type GroupedData = Tuple2[Timestamp, List[IntervalWindow]]

  def timestamp2WindowEpoch(timestamp: Timestamp, windowSize: Long): Long = {
    epoch2WindowEpoch(timestamp.getTime, windowSize)
  }

  def epoch2WindowEpoch(epoch: Long, windowSize: Long): Long = {
    windowSize*(epoch/windowSize)
  }

  def baseSeries(startEpoch: Long, endEpoch: Long, step: Long): Iterable[IntervalWindow] = {
    longRange(startEpoch, endEpoch + 1, step).map(
      x => new IntervalWindow(
        new Timestamp(x),
        new Timestamp(x)
      )
    )  // end should be included if it happens to match window
  }

  def longRange(start: Long, end: Long, step: Long): Iterable[Long] = {
    val outputSize = (end - start + 1)/step
    (0L to outputSize).map(start + _*step).filter(_<end)
  }

  def datapoint2IntervalWindow(datapoint: Datapoint, windowSize: Long): IntervalWindow = {
    new IntervalWindow(
      new Timestamp(timestamp2WindowEpoch(datapoint.timestamp, windowSize)),
      datapoint.timestamp,
      Some(datapoint.value)
    )
  }

  //def forwardFill[A](data: Iterable[Tuple2[Timestamp, List[IntervalWindow[A]]]]): Iterable[Tuple2[Timestamp, List[IntervalWindow[A]]]] = {
  def forwardFill(data: List[GroupedData]): List[GroupedData] = {

    def workOnItem[A](previous: GroupedData, current: GroupedData): GroupedData = {
      // a bit tricky..

      // By design previous can be
      // IntervalWindow(something, None)
      // only at the first item of the iterable
      if (previous._2.length == 0) {
        current
      } else {
        val latestPrevious = List(previous._2.maxBy(_.timestamp.getTime))
        assert(latestPrevious.length == 1)

        // If current is None, then add last of previous windows
        // If current is not None, then add last of previous windows
        // So always add last of previous windows
        new Tuple2(
          current._1,
          List.concat(current._2, latestPrevious)
            .filter(_.value != None)
            .sortWith(_.timestamp.getTime < _.timestamp.getTime)  // Sort to make testing easier
        )
      }
    }

    // Initial timestamp is ignored
    val zeroEpoch = new Timestamp(0)
    data.sortWith((x, y) => x._1
      .getTime < y._1.getTime)
      .scanLeft((zeroEpoch, List(IntervalWindow(zeroEpoch, zeroEpoch, None))))(workOnItem(_, _))
      .drop(1) // The artificial seed value needs to be dropped
  }

  def evaluateWindow(window: GroupedData): Window = {
    // If list length is zero then output is NaN
    Window(window._1, window._2.map(_.value).flatten.sum/window._2.length)
  }

  def evaluateDatapoints(inputData: List[Datapoint], startEpoch: Long, endEpoch: Long, windowSize: Long): List[Window] = {
    val intervalWindows = inputData.map(datapoint2IntervalWindow(_, windowSize))

    val base = baseSeries(startEpoch, endEpoch, windowSize)

    val groupedData: List[GroupedData] = List.concat(intervalWindows, base).groupBy(x => x.windowstart).toList

    val forwardFilled = forwardFill(groupedData).sortWith(_._1.getTime < _._1.getTime)

    // Filter needed since usually the first value in input data is before startEpoch
    forwardFilled.map(evaluateWindow).filter(_.windowstart.getTime>=startEpoch)
  }
}
