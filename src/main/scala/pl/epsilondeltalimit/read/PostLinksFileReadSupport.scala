package pl.epsilondeltalimit.read

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{quarter, to_date, year}
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.epsilondeltalimit.withColumnNamesNormalized
import pl.epsilondeltalimit.storage.XmlFileStorage

object PostLinksFileReadSupport {
  private[this] val logger = Logger.getLogger(PostLinksFileReadSupport.getClass.getSimpleName)

  private[this] val Schema: StructType = StructType(Array(
    StructField("_Id", LongType),
    StructField("_PostHistoryTypeId", IntegerType),
    StructField("_PostId", LongType),
    StructField("_RevisionGUID", StringType),
    StructField("_CreationDate", StringType),
    StructField("_UserId", LongType),
    StructField("_Text", StringType),
    StructField("_Comment", StringType)
  ))

  def read(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    logger.warn(s"Loading data from file: $path.")
    val dataFromFile = new XmlFileStorage(spark).readFromXmlFile("row", Schema, path)
    withColumnNamesNormalized(dataFromFile)
      .select($"id",
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
