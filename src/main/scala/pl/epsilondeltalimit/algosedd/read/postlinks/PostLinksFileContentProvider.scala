package pl.epsilondeltalimit.algosedd.read.postlinks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Dep, Transformation}

object PostLinksFileContentProvider extends Transformation with Logging {

  private[this] val Schema: StructType = StructType(
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

  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("postLinks")(c.get[SparkSession]("spark"), c.get[String]("pathToPostLinksFile")) {
        (spark, pathToPostLinksFile) =>
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
    }

}
