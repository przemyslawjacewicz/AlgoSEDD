package pl.epsilondeltalimit.read

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.epsilondeltalimit.withColumnNamesNormalized
import pl.epsilondeltalimit.storage.XmlFileStorage

object UsersFileReadSupport {
  private[this] val logger = Logger.getLogger(UsersFileReadSupport.getClass.getSimpleName)

  private[this] val Schema: StructType = StructType(Array(
    StructField("_Id", LongType),
    StructField("_Reputation", IntegerType),
    StructField("_CreationDate", StringType),
    StructField("_DisplayName", StringType),
    StructField("_LastAccessDate", StringType),
    StructField("_WebsiteUrl", StringType),
    StructField("_Location", StringType),
    StructField("_AboutMe", StringType),
    StructField("_Views", LongType),
    StructField("_UpVotes", LongType),
    StructField("_DownVotes", LongType),
    StructField("_AccountId", LongType)
  ))

  def read(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    logger.warn(s"Loading data from file: $path.")
    val dataFromFile = new XmlFileStorage(spark).readFromXmlFile("row", Schema, path)
    withColumnNamesNormalized(dataFromFile)
      .select($"id",
        $"reputation",
        to_date($"creation_date".cast(TimestampType)).as("creation_date"),
        $"display_name",
        to_date($"last_access_date".cast(TimestampType)).as("last_access_date"),
        $"website_url",
        $"location",
        $"about_me",
        $"views",
        $"up_votes",
        $"down_votes",
        $"account_id")
  }
}
