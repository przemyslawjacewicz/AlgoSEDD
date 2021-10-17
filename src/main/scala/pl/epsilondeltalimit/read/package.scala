package pl.epsilondeltalimit

import com.google.common.base.CaseFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

package object epsilondeltalimit {
  def withColumnNamesNormalized(df: DataFrame): DataFrame = {
    def dropPrefixAndChangeCase(name: String) = {
      CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name.drop(1))
    }

    val columns = df.columns.map(name => col(name).as(dropPrefixAndChangeCase(name)))
    df.select(columns: _*)
  }
}
