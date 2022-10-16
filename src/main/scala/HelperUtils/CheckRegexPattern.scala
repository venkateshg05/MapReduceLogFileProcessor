package HelperUtils

import java.util.regex.Pattern

object CheckRegexPattern {

  def checkPattern(msg: String) =
    /*
    Checks if the log message has injected pattern
    by matching against the pattern in the config file
    */
    val pattern = Pattern.compile(Parameters.injectedPattern)
    val msgHasPattern = pattern.matcher(msg).find()
    msgHasPattern

}
