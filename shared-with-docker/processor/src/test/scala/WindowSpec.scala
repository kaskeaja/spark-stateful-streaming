package deltaLakeProcessor

import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._

import java.sql.Timestamp

class WindowSpec extends AnyFlatSpec with should.Matchers {

  "timestamp2WindowEpoch" should "return timestamps that are along windowSize boundaries" in {
    deltaLakeProcessor.timestamp2WindowEpoch(new Timestamp(10), 11) should be (0)
    deltaLakeProcessor.timestamp2WindowEpoch(new Timestamp(10), 9) should be (9)
    deltaLakeProcessor.timestamp2WindowEpoch(new Timestamp(11), 9) should be (9)
    deltaLakeProcessor.timestamp2WindowEpoch(new Timestamp(17), 9) should be (9)
    deltaLakeProcessor.timestamp2WindowEpoch(new Timestamp(18), 9) should be (18)
    deltaLakeProcessor.timestamp2WindowEpoch(new Timestamp(10), 10) should be (10)
  }

  "longRange" should "be equal to Range" in {
    deltaLakeProcessor.longRange(1, 10, 2).map(x => x.toInt) should be (Range(1, 10, 2).toList)
    deltaLakeProcessor.longRange(5, 15, 2).map(x => x.toInt) should be (Range(5, 15, 2).toList)
    deltaLakeProcessor.longRange(5, 16, 2).map(x => x.toInt) should be (Range(5, 16, 2).toList)
    deltaLakeProcessor.longRange(5, 14, 3).map(x => x.toInt) should be (Range(5, 14, 3).toList)

    deltaLakeProcessor.longRange(15, 200, 15).map(x => x.toInt) should be (Range(15, 200, 15).toList)
    // Systematic sweep
    for (start <- 0 to 10) {
      for (end <- 11 to 50) {
        for (step <- 1 to 10) {
          //println(s"$start to $end with step $step")
          deltaLakeProcessor.longRange(start, end, step).map(x => x.toInt) should be (Range(start, end, step).toList)
        }
      }
    }
  }
  /*it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[Int]
    a [NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    } 
  }
  */

  "forwardFill" should "return forward filled data" in {
    val windowSize: Long = 3

    val inputData = List(
      deltaLakeProcessor.Datapoint(new Timestamp(0), 11),
      deltaLakeProcessor.Datapoint(new Timestamp(1), 22),
      deltaLakeProcessor.Datapoint(new Timestamp(8), 88),
      deltaLakeProcessor.Datapoint(new Timestamp(9), 99),
      deltaLakeProcessor.Datapoint(new Timestamp(11), 1111),
      deltaLakeProcessor.Datapoint(new Timestamp(12), 1212),
    )

    val base = deltaLakeProcessor.baseSeries(0, 16, windowSize)

    val groupedData = List.concat(
        inputData.map(deltaLakeProcessor.datapoint2IntervalWindow(_, windowSize)),
        base
      ).groupBy(x => x.windowstart).toList

    val forwardFilled = deltaLakeProcessor
      .forwardFill(groupedData)
      .sortWith(_._1.getTime < _._1.getTime)

    println("---------------")
    println("---------------")

    def pretty(data: deltaLakeProcessor.GroupedData): Unit = {
      println("####")
      println(data._1)
      data._2.map(println(_))
    }

    forwardFilled.map(pretty(_))

    val refValueForwardFilled = List(
      /*new deltaLakeProcessor.GroupedData(
        new Timestamp(0),
        List(
          new deltaLakeProcessor.IntervalWindow(new Timestamp(0), new Timestamp(0))
        )
      ), // Zero case*/
      new deltaLakeProcessor.GroupedData(
        new Timestamp(0),
        List(
          new deltaLakeProcessor.IntervalWindow(new Timestamp(0), new Timestamp(0), Some(11.0)),
          new deltaLakeProcessor.IntervalWindow(new Timestamp(0), new Timestamp(1), Some(22.0))
        )
      ), // First case
      new deltaLakeProcessor.GroupedData(
        new Timestamp(3),
        List(
          new deltaLakeProcessor.IntervalWindow(new Timestamp(0), new Timestamp(1), Some(22.0))
        )
      ), // Third case
      new deltaLakeProcessor.GroupedData(
        new Timestamp(6),
        List(
          new deltaLakeProcessor.IntervalWindow(new Timestamp(0), new Timestamp(1), Some(22.0)),
          new deltaLakeProcessor.IntervalWindow(new Timestamp(6), new Timestamp(8), Some(88.0))
        )
      ), // Fourth case
      new deltaLakeProcessor.GroupedData(
        new Timestamp(9),
        List(
          new deltaLakeProcessor.IntervalWindow(new Timestamp(6), new Timestamp(8), Some(88.0)),
          new deltaLakeProcessor.IntervalWindow(new Timestamp(9), new Timestamp(9), Some(99.0)),
          new deltaLakeProcessor.IntervalWindow(new Timestamp(9), new Timestamp(11), Some(1111.0))
        )
      ), // Fourth case
      new deltaLakeProcessor.GroupedData(
        new Timestamp(12),
        List(
          new deltaLakeProcessor.IntervalWindow(new Timestamp(9), new Timestamp(11), Some(1111.0)),
          new deltaLakeProcessor.IntervalWindow(new Timestamp(12), new Timestamp(12), Some(1212.0))
        )
      ), // Fifth case
      new deltaLakeProcessor.GroupedData(
        new Timestamp(15),
        List(
          new deltaLakeProcessor.IntervalWindow(new Timestamp(12), new Timestamp(12), Some(1212.0))
        )
      ), // Fifth case
    )
    forwardFilled should be (refValueForwardFilled)

    val evaluatedWindows = forwardFilled.map(deltaLakeProcessor.evaluateWindow)
    print("------")
    print("------")
    evaluatedWindows.map(println)
  
    val refValueEvaluatedWindows = List(
      //deltaLakeProcessor.Window(new Timestamp(0), 0.0),
      deltaLakeProcessor.Window(new Timestamp(0), 16.5),
      deltaLakeProcessor.Window(new Timestamp(3), 22.0),
      deltaLakeProcessor.Window(new Timestamp(6), 55.0),
      deltaLakeProcessor.Window(new Timestamp(9), 432.6666666666667),
      deltaLakeProcessor.Window(new Timestamp(12), 1161.5),
      deltaLakeProcessor.Window(new Timestamp(15), 1212.0),
    )

    evaluatedWindows should be (refValueEvaluatedWindows)

    //
    // End to end approach
    //

    deltaLakeProcessor.evaluateDatapoints(inputData, 0, 16, 3) should be (refValueEvaluatedWindows)
  }
}
