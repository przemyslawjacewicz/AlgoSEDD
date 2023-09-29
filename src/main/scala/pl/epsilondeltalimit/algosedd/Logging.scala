package pl.epsilondeltalimit.algosedd

import org.apache.log4j.Logger

trait Logging {
  val logger: Logger = Logger.getLogger(this.getClass.getSimpleName)
}
