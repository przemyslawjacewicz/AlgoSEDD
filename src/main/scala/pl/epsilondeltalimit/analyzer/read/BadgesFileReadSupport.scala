package pl.epsilondeltalimit.analyzer.read

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.analyzer.support.storage.XmlFileStorage

object BadgesFileReadSupport {
  private[this] val logger = Logger.getLogger(BadgesFileReadSupport.getClass.getSimpleName)

  private[this] val Schema: StructType = StructType(Array(
    StructField("_Id", LongType),
    StructField("_UserId", IntegerType),
    StructField("_Name", StringType),
    StructField("_Date", StringType),
    StructField("_Class", LongType),
    StructField("_TagBased", BooleanType)
  ))

  def read(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    logger.warn(s"Loading data from file: $path.")
    val dataFromFile = new XmlFileStorage(spark).readFromXmlFile("row", Schema, path)
    withColumnNamesNormalized(dataFromFile)
      .select($"id",
        $"user_id",
        $"name",
        to_date($"date".cast(TimestampType)).as("date"),
        $"class",
        $"tag_based")
  }
}
