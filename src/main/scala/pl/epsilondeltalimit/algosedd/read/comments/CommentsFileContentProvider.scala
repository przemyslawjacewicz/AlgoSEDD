package pl.epsilondeltalimit.algosedd.read.comments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.Transformations.Transformation
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object CommentsFileContentProvider extends Transformation with Logging {
  private[this] val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_PostId", LongType),
      StructField("_Score", IntegerType),
      StructField("_Text", StringType),
      StructField("_CreationDate", StringType),
      StructField("_UserId", LongType)
    ))

  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("comments")(c.get[SparkSession]("spark"), c.get[String]("pathToCommentsFile")) {
        (spark, pathToCommentsFile) =>
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
    }

}
