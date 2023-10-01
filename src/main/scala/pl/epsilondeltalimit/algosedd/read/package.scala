package pl.epsilondeltalimit.algosedd

import com.google.common.base.CaseFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

package object read {

  object implicits {

    implicit class SparkSessionImplicits(spark: SparkSession) {
      def readFromXmlFile(schema: StructType, path: String): DataFrame =
        spark.read
          .format("xml")
          .option("rowTag", "row")
          .option("inferSchema", value = false)
          .schema(schema)
          .load(path)
    }

    implicit class DataFrameImplicits(df: DataFrame) {
      def withColumnNamesNormalized: DataFrame = {
        def dropPrefixAndChangeCase(name: String) =
          CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name.drop(1))

        val columns = df.columns.map(name => col(name).as(dropPrefixAndChangeCase(name)))
        df.select(columns: _*)
      }
    }
  }

}
