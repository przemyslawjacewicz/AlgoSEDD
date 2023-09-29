package pl.epsilondeltalimit.algosedd.read.users

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.algosedd.storage.XmlFileStorage
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Dep, Transformation}

object UsersFileContentProvider extends Transformation with Logging {

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

  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("users")(c.get[SparkSession]("spark"), c.get[String]("pathToUsersFile")) { (spark, pathToUsersFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToUsersFile.")

        new XmlFileStorage(spark).readFromXmlFile("row", Schema, pathToUsersFile)
          .withColumnNamesNormalized
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

}
