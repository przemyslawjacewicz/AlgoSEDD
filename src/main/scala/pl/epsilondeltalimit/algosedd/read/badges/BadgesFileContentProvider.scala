package pl.epsilondeltalimit.algosedd.read.badges

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.algosedd._
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.implicits._

object BadgesFileContentProvider extends (Catalog => Result[DataFrame]) with Logging {

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_UserId", IntegerType),
      StructField("_Name", StringType),
      StructField("_Date", StringType),
      StructField("_Class", LongType),
      StructField("_TagBased", BooleanType)
    ))

  override def apply(c: Catalog): Result[DataFrame] = {
    implicit val _c: Catalog = c

    "spark"
      .as[SparkSession]
      .map2("pathToBadgesFile".as[String]) { (spark, pathToBadgesFile) =>
        logger.warn(s"Loading data from file: $pathToBadgesFile.")

        spark
          .readFromXmlFile(Schema, pathToBadgesFile)
          .withColumnNamesNormalized
          .select(
            col("id"),
            col("user_id"),
            col("name"),
            to_date(col("date").cast(TimestampType)).as("date"),
            col("class"),
            col("tag_based")
          )
      }
      .as("badges")
  }

}
