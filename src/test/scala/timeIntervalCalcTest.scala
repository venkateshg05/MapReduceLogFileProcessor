import HelperUtils.{Parameters, GetTimeInterval}

import org.scalatest.{funsuite}
import java.time.{Duration, LocalTime}
import scala.io.BufferedSource

class timeIntervalCalcTest extends funsuite.AnyFunSuite {
  test("getTimeInterval") {
    val ts = LocalTime.parse("12:59:00.000")
    val st = LocalTime.parse(Parameters.startTime)
    val interval = Parameters.timeInterval
    val en = LocalTime.parse(Parameters.endTime)
    val totalDuration = Duration.between(st, en).toMinutes()
    assert(
      GetTimeInterval.getTimeInterval(st, 0, totalDuration.toInt/interval, ts, interval) == "12:59, 13:00"
    )
  }
}
