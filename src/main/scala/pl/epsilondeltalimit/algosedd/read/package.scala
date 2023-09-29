package pl.epsilondeltalimit.algosedd

import com.google.common.base.CaseFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

package object read {

  object implicits {

    implicit class DataFrameImplicits(df: DataFrame) {
      def withColumnNamesNormalized: DataFrame = {
        def dropPrefixAndChangeCase(name: String) = {
          CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name.drop(1))
        }

        val columns = df.columns.map(name => col(name).as(dropPrefixAndChangeCase(name)))
        df.select(columns: _*)
      }
    }
  }

}
