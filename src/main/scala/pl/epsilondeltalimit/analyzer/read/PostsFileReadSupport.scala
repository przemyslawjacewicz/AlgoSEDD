package pl.epsilondeltalimit.analyzer.read

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.analyzer.support.storage.XmlFileStorage

object PostsFileReadSupport {
  private[this] val logger = Logger.getLogger(PostsFileReadSupport.getClass.getSimpleName)

  private[this] val Schema: StructType = StructType(Array(
    StructField("_Id", LongType),
    StructField("_PostTypeId", IntegerType),
    StructField("_AcceptedAnswerId", LongType),
    StructField("_ParentId", LongType),
    StructField("_CreationDate", StringType),
    StructField("_Score", IntegerType),
    StructField("_ViewCount", IntegerType),
    StructField("_Body", StringType),
    StructField("_OwnerUserId", LongType),
    StructField("_LastEditorUserId", LongType),
    StructField("_LastEditDate", StringType),
    StructField("_LastActivityDate", StringType),
    StructField("_Title", StringType),
    StructField("_Tags", StringType),
    StructField("_AnswerCount", IntegerType),
    StructField("_CommentCount", IntegerType),
    StructField("_FavoriteCount", IntegerType),
    StructField("_ClosedDate", StringType),
    StructField("_CommunityOwnedDate", StringType)
  ))

  def read(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    logger.warn(s"Loading data from file: $path.")
    val dataFromFile = new XmlFileStorage(spark).readFromXmlFile("row", Schema, path)
    withColumnNamesNormalized(dataFromFile)
      .select($"id",
        $"post_type_id",
        $"parent_id",
        $"accepted_answer_id",
        to_date($"creation_date".cast(TimestampType)).as("creation_date"),
        $"score",
        $"view_count",
        $"body",
        $"owner_user_id",
        $"last_editor_user_id",
        to_date($"last_edit_date".cast(TimestampType)).as("last_edit_date"),
        to_date($"last_activity_date".cast(TimestampType)).as("last_activity_date"),
        $"title",
        array_distinct(
          split(regexp_replace(
            regexp_replace($"tags", "^<", ""), ">$", ""),
            "><")).as("tags"),
        $"answer_count",
        $"comment_count",
        $"favorite_count",
        to_date($"closed_date".cast(TimestampType)).as("closed_date"),
        to_date($"community_owned_date".cast(TimestampType)).as("community_owned_date"))
      .withColumn("year", year($"creation_date"))
      .withColumn("quarter", quarter($"creation_date"))
  }
}
