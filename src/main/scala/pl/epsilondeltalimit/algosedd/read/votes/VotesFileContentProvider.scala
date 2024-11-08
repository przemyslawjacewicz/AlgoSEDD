package pl.epsilondeltalimit.algosedd.read.votes

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.algosedd._
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.implicits._

object VotesFileContentProvider extends (Catalog => Result[DataFrame]) with Logging {

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_PostId", LongType),
      StructField("_VoteTypeId", IntegerType),
      StructField("_UserId", LongType),
      StructField("_CreationDate", StringType)
    ))

  override def apply(c: Catalog): Result[DataFrame] = {
    implicit val _c: Catalog = c

    "spark"
      .as[SparkSession]
      .map2("pathToVotesFile".as[String]) { (spark, pathToVotesFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToVotesFile.")

        spark
          .readFromXmlFile(Schema, pathToVotesFile)
          .withColumnNamesNormalized
          .select(
            $"id",
            $"post_id",
            $"vote_type_id",
            $"user_id",
            to_date($"creation_date".cast(TimestampType)).as("creation_date")
          )
          .withColumn("year", year($"creation_date"))
          .withColumn("quarter", quarter($"creation_date"))
      }
      .as("votes")
  }

}
