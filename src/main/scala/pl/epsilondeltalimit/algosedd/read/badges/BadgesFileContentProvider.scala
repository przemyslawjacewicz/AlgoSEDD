package pl.epsilondeltalimit.algosedd.read.badges

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.Transformations.Transformation
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object BadgesFileContentProvider extends Transformation with Logging {

  private[this] val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_UserId", IntegerType),
      StructField("_Name", StringType),
      StructField("_Date", StringType),
      StructField("_Class", LongType),
      StructField("_TagBased", BooleanType)
    ))

  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("badges")(c.get[SparkSession]("spark"), c.get[String]("pathToBadgesFile")) { (spark, pathToBadgesFile) =>
        logger.warn(s"Loading data from file: $pathToBadgesFile.")

        spark
          .readFromXmlFile(Schema, pathToBadgesFile)
          .withColumnNamesNormalized
          .select(
            col("id"),
            col("user_id"),
            col("name"),
            to_date(col("date").cast(TimestampType)).as("date"),
            col("class"),
            col("tag_based")
          )
      }
    }
}
