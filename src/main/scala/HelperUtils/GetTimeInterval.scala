package HelperUtils

import scala.annotation.tailrec
import java.time.{Duration, LocalTime}
import java.util
import java.util.regex.Pattern

object GetTimeInterval {

  @tailrec
  def getTimeInterval(startTime: LocalTime, lowerBound: Int, upperBound: Int, time: LocalTime, interval: Int): String = {

    // time not with in interval
    if (time.isBefore(startTime) || time.isAfter(startTime.plusMinutes(upperBound * interval))) {
      "Time not within bounds"
    }
    else {

      // calculate the mid index
      val sum = lowerBound + upperBound
      val mid = if sum % 2 == 0 then (sum) / 2 else (sum / 2) + 1

      // calculate the actual time interval for the begin, mid & end indices
      val intervalBegin = startTime.plusMinutes(lowerBound * interval)
      val intervalEnd = startTime.plusMinutes(upperBound * interval)
      val intervalMid = startTime.plusMinutes(mid * interval)

      // Terminating condition - log time lies in previous interval [begin, end]
      if (mid >= upperBound)
        intervalBegin.plusMinutes(1).toString + ", " + intervalEnd.toString
      else {
        // Terminating condition - log time lies in interval end boundaries [begin, end]
        if (time.compareTo(intervalEnd) == 0) {
          intervalEnd.minusMinutes(interval - 1).toString + ", " + intervalEnd.toString
        }
        else if (time.compareTo(intervalMid) == 0) {
          intervalMid.minusMinutes(interval - 1).toString + ", " + intervalMid.toString
        }
        // smaller interval possible, continue binary search
        else if (time.isAfter(intervalBegin) && time.isBefore(intervalMid)) {
          getTimeInterval(startTime, lowerBound, mid, time, interval)
        } else {
          getTimeInterval(startTime, mid, upperBound, time, interval)
        }
      }
    }
  }

}
