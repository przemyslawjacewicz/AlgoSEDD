package pl.epsilondeltalimit.algosedd.read.users

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.algosedd._
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.implicits._

object UsersFileContentProvider extends (Catalog => Result[DataFrame]) with Logging {

  private val Schema: StructType = StructType(
    Array(
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

  override def apply(c: Catalog): Result[DataFrame] = {
    implicit val _c: Catalog = c

    "spark"
      .as[SparkSession]
      .map2("pathToUsersFile".as[String]) { (spark, pathToUsersFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToUsersFile.")

        spark
          .readFromXmlFile(Schema, pathToUsersFile)
          .withColumnNamesNormalized
          .select(
            $"id",
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
            $"account_id"
          )
      }
      .as("users")
  }

}
