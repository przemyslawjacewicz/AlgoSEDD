package pl.epsilondeltalimit.read

import org.apache.log4j.Logger
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.epsilondeltalimit.withColumnNamesNormalized
import pl.epsilondeltalimit.storage.XmlFileStorage

object TagsFileReadSupport {
  private[this] val logger = Logger.getLogger(TagsFileReadSupport.getClass.getSimpleName)

  private[this] val Schema: StructType = StructType(Array(
    StructField("_Id", LongType),
    StructField("_TagName", StringType),
    StructField("_Count", LongType),
    StructField("_ExcerptPostId", LongType),
    StructField("_WikiPostId", LongType)
  ))

  def read(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    logger.warn(s"Loading data from file: $path.")
    val dataFromFile = new XmlFileStorage(spark).readFromXmlFile("row", Schema, path)
    withColumnNamesNormalized(dataFromFile)
      .select($"id",
        $"tag_name",
        $"count",
        $"excerpt_post_id",
        $"wiki_post_id")
  }
}
