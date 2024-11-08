package pl.epsilondeltalimit

import org.apache.hadoop.fs.Path

package object algosedd {
  object implicits {
    implicit class StringOps(val str: String) extends AnyVal {
      def /(s: String): String =
        new Path(str, s).toString
    }
  }
}
