package pl.epsilondeltalimit.algosedd.read.badges

import cats.Monad
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.algosedd._
import pl.epsilondeltalimit.algosedd.implicits._
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.{Dep, Result}
import pl.epsilondeltalimit.dep.transformation.ResultTransformationWithImplicitCatalog

object BadgesFileContentProvider extends ResultTransformationWithImplicitCatalog[DataFrame] with Logging {

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_UserId", IntegerType),
      StructField("_Name", StringType),
      StructField("_Date", StringType),
      StructField("_Class", LongType),
      StructField("_TagBased", BooleanType)
    ))

  override def apply(implicit c: Catalog): Result[DataFrame] =
    Monad[Dep]
      .map2("spark".as[SparkSession], "pathToBadgesFile".as[String]) { (spark, pathToBadgesFile) =>
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
