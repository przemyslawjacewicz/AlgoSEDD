package pl.epsilondeltalimit.algosedd.read.badges

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object BadgesFileContentProvider extends PutTransformationWithImplicitCatalog with Logging {
  import Dep.implicits._

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_UserId", IntegerType),
      StructField("_Name", StringType),
      StructField("_Date", StringType),
      StructField("_Class", LongType),
      StructField("_TagBased", BooleanType)
    ))

  override def apply(implicit c: Catalog): Dep[_] =
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
