package pl.epsilondeltalimit.algosedd.read.posts

import cats.Monad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.algosedd.{Logging, PutTransformationWithImplicitCatalogM}
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object PostsFileContentProvider extends PutTransformationWithImplicitCatalogM with Logging {
  import Dep.implicits._
  import pl.epsilondeltalimit.algosedd._
  private val Schema: StructType = StructType(
    Array(
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

  def apply_(implicit c: Catalog): Dep[_] =
    "spark"
      .as[SparkSession]
      .map2("pathToPostsFile".as[String]) { (spark, pathToPostsFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToPostsFile.")

        spark
          .readFromXmlFile(Schema, pathToPostsFile)
          .withColumnNamesNormalized
          .select(
            $"id",
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
            array_distinct(split(regexp_replace(regexp_replace($"tags", "^<", ""), ">$", ""), "><")).as("tags"),
            $"answer_count",
            $"comment_count",
            $"favorite_count",
            to_date($"closed_date".cast(TimestampType)).as("closed_date"),
            to_date($"community_owned_date".cast(TimestampType)).as("community_owned_date")
          )
          .withColumn("year", year($"creation_date"))
          .withColumn("quarter", quarter($"creation_date"))
      }
      .as("posts")

  override implicit val id: String = "posts"

  override def apply(implicit c: Catalog): Dep[_] =
    Monad[Dep].map2("spark".as[SparkSession], "pathToPostsFile".as[String]) { (spark, pathToPostsFile) =>
      import spark.implicits._

      logger.warn(s"Loading data from file: $pathToPostsFile.")

      spark
        .readFromXmlFile(Schema, pathToPostsFile)
        .withColumnNamesNormalized
        .select(
          $"id",
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
          array_distinct(split(regexp_replace(regexp_replace($"tags", "^<", ""), ">$", ""), "><")).as("tags"),
          $"answer_count",
          $"comment_count",
          $"favorite_count",
          to_date($"closed_date".cast(TimestampType)).as("closed_date"),
          to_date($"community_owned_date".cast(TimestampType)).as("community_owned_date")
        )
        .withColumn("year", year($"creation_date"))
        .withColumn("quarter", quarter($"creation_date"))
    }
      .as(id)

}
