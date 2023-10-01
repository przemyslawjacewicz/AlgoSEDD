package pl.epsilondeltalimit.algosedd.read.votes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{quarter, to_date, year}
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.XmlFileStorage
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Dep, Transformation}

object VotesFileContentProvider extends Transformation with Logging {

  private[this] val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_PostId", LongType),
      StructField("_VoteTypeId", IntegerType),
      StructField("_UserId", LongType),
      StructField("_CreationDate", StringType)
    ))

  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("votes")(c.get[SparkSession]("spark"), c.get[String]("pathToVotesFile")) { (spark, pathToVotesFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToVotesFile.")

        new XmlFileStorage(spark)
          .readFromFile("row", Schema, pathToVotesFile)
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
    }

}
