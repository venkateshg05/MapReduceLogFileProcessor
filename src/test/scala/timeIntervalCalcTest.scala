import HelperUtils.{Parameters, GetTimeInterval}

import org.scalatest.{funsuite}
import java.time.{Duration, LocalTime}

class timeIntervalCalcTest extends funsuite.AnyFunSuite {
  test("getTimeInterval") {
    val ts = LocalTime.parse("12:57:52.000")
    val st = LocalTime.parse("12:00")
    val interval = 5
    val en = LocalTime.parse("14:30")
    val totalDuration = Duration.between(st, en).toMinutes()
    assert(
      GetTimeInterval.getTimeInterval(st, 0, totalDuration.toInt/interval, ts, interval) == "12:56, 13:00"
    )
  }
  test("getTimeIntervalBoundaryCase") {
    val ts = LocalTime.parse("12:50:00.000")
    val st = LocalTime.parse("12:00")
    val interval = 10
    val en = LocalTime.parse("14:00")
    val totalDuration = Duration.between(st, en).toMinutes()
    assert(
      GetTimeInterval.getTimeInterval(st, 0, totalDuration.toInt / interval, ts, interval) == "12:41, 12:50"
    )
  }
  test("getTimeIntervalOutOfBounds") {
    val ts = LocalTime.parse("11:59:00.000")
    val st = LocalTime.parse("12:00")
    val interval = 5
    val en = LocalTime.parse("14:00")
    val totalDuration = Duration.between(st, en).toMinutes()
    assert(
      GetTimeInterval.getTimeInterval(st, 0, totalDuration.toInt / interval, ts, interval) == "Time not within bounds"
    )
  }
}
