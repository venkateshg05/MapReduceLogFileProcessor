package HelperUtils

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters.*

/*
* This module obtains configuration parameter values from application.conf and converts them
* into appropriate scala types.
* */
object Parameters:
  private val logger = CreateLogger(classOf[Parameters.type])
  val config = ObtainConfigReference("logsProcessor") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  //It returns a function that takes the name of config entry and obtains the value of this entry if it exists
  //or it logs a warning message if it is absent and returns a default value
  private def func4Parameter[T](defaultVal: T, f: String => T): String => T =
    (pName: String) => Try(f(s"logsProcessor.$pName")) match {
      case Success(value) => value
      case Failure(exception) => logger.warn(s"No config parameter $pName is provided. Defaulting to $defaultVal")
        defaultVal
    }
  end func4Parameter

  //Type match is used to dependently type configuration parameter values
  //based on the default input values of the specific config parameter.
  type ConfigType2Process[T] = T match
    case Int => Int
    case Long => Long
    case String => String
    case Double => Double

  //in this dependently typed function a typesafe config API method is invoked
  //whose name and return value corresponds to the type of the type parameter, T
  private def getParam[T](pName: String, defaultVal: T): ConfigType2Process[T] =
    defaultVal match {
      case v: Int => func4Parameter(v, config.getInt)(pName)
      case v: Long => func4Parameter(v, config.getLong)(pName)
      case v: String => func4Parameter(v, config.getString)(pName)
      case v: Double => func4Parameter(v, config.getDouble)(pName)
    }
  end getParam

  val startTime = getParam("StartTime", "12:00")
  val endTime = getParam("EndTime", "14:00")
  val timeInterval = getParam("TimeInterval", 5)
  val injectedPattern = getParam("Pattern", "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
  val numMappers = getParam("NumMappers", "1")
  val numReducers = getParam("NumReducers", "1")




