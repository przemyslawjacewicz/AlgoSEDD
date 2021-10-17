package pl.epsilondeltalimit.read

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{quarter, to_date, year}
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.epsilondeltalimit.withColumnNamesNormalized
import pl.epsilondeltalimit.storage.XmlFileStorage

object VotesFileReadSupport {
  private[this] val logger = Logger.getLogger(VotesFileReadSupport.getClass.getSimpleName)

  private[this] val Schema: StructType = StructType(Array(
    StructField("_Id", LongType),
    StructField("_PostId", LongType),
    StructField("_VoteTypeId", IntegerType),
    StructField("_UserId", LongType),
    StructField("_CreationDate", StringType)
  ))

  def read(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    logger.warn(s"Loading data from file: $path.")
    val dataFromFile = new XmlFileStorage(spark).readFromXmlFile("row", Schema, path)
    withColumnNamesNormalized(dataFromFile)
      .select($"id",
        $"post_id",
        $"vote_type_id",
        $"user_id",
        to_date($"creation_date".cast(TimestampType)).as("creation_date"))
      .withColumn("year", year($"creation_date"))
      .withColumn("quarter", quarter($"creation_date"))
  }
}
