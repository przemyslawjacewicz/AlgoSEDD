package pl.epsilondeltalimit.algosedd.read.comments

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.algosedd._
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.implicits._

object CommentsFileContentProvider extends (Catalog => Result[DataFrame]) with Logging {

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_PostId", LongType),
      StructField("_Score", IntegerType),
      StructField("_Text", StringType),
      StructField("_CreationDate", StringType),
      StructField("_UserId", LongType)
    ))

  override def apply(c: Catalog): Result[DataFrame] = {
    implicit val _c: Catalog = c

    "spark"
      .as[SparkSession]
      .map2("pathToCommentsFile".as[String]) { (spark, pathToCommentsFile) =>
        logger.warn(s"Loading data from file: $pathToCommentsFile.")

        spark
          .readFromXmlFile(Schema, pathToCommentsFile)
          .withColumnNamesNormalized
          .select(
            col("id"),
            col("post_id"),
            col("score"),
            col("text"),
            to_date(col("creation_date").cast(TimestampType)).as("creation_date"),
            col("user_id")
          )
          .withColumn("year", year(col("creation_date")))
          .withColumn("quarter", quarter(col("creation_date")))
      }
      .as("comments")
  }

}
