package pl.epsilondeltalimit.algosedd.read.votes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{quarter, to_date, year}
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object VotesFileContentProvider extends PutTransformationWithImplicitCatalog with Logging {
  import Dep.implicits._

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_PostId", LongType),
      StructField("_VoteTypeId", IntegerType),
      StructField("_UserId", LongType),
      StructField("_CreationDate", StringType)
    ))

  override def apply(implicit c: Catalog): Dep[_] =
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
