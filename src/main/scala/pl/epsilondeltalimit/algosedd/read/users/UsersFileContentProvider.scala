package pl.epsilondeltalimit.algosedd.read.users

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object UsersFileContentProvider extends PutTransformationWithImplicitCatalog with Logging {
  import Dep.implicits._

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

  override def apply(implicit c: Catalog): Dep[_] =
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
