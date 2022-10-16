import HelperUtils.{CheckRegexPattern, Parameters}
import org.scalatest.funsuite

class regexPatternMatcherTest extends funsuite.AnyFunSuite {

  test("checkIfPatternExists") {
    val stringToCheck = "WV1)ZxWcf2bf1ae2bf2Y7sU6iR7p?65}DNu"
    assert(
      CheckRegexPattern.checkPattern(stringToCheck) == true
    )
  }

  test("checkIfPatternNotExists") {
    val stringToCheck = "abcdef"
    assert(
      CheckRegexPattern.checkPattern(stringToCheck) == false
    )
  }

}
