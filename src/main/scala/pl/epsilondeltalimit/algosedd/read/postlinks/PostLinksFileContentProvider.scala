package pl.epsilondeltalimit.algosedd.read.postlinks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.algosedd._
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.implicits._

object PostLinksFileContentProvider extends (Catalog => Result[DataFrame]) with Logging {

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_PostHistoryTypeId", IntegerType),
      StructField("_PostId", LongType),
      StructField("_RevisionGUID", StringType),
      StructField("_CreationDate", StringType),
      StructField("_UserId", LongType),
      StructField("_Text", StringType),
      StructField("_Comment", StringType)
    ))

  override def apply(c: Catalog): Result[DataFrame] = {
    implicit val _c: Catalog = c

    "spark"
      .as[SparkSession]
      .map2("pathToPostLinksFile".as[String]) { (spark, pathToPostLinksFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToPostLinksFile.")

        spark
          .readFromXmlFile(Schema, pathToPostLinksFile)
          .withColumnNamesNormalized
          .select(
            $"id",
            $"post_history_type_id",
            $"post_id",
            $"revision_g_u_i_d".as("revision_guid"),
            to_date($"creation_date".cast(TimestampType)).as("creation_date"),
            $"user_id",
            $"text",
            $"comment"
          )
          .withColumn("year", year($"creation_date"))
          .withColumn("quarter", quarter($"creation_date"))
      }
      .as("postLinks")
  }

}
