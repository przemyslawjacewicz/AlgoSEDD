package pl.epsilondeltalimit.algosedd.read.badges

import cats.Monad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.{Logging, PutTransformationWithImplicitCatalogM}
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object BadgesFileContentProvider extends PutTransformationWithImplicitCatalogM with Logging {
  import Dep.implicits._
import pl.epsilondeltalimit.algosedd._

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_UserId", IntegerType),
      StructField("_Name", StringType),
      StructField("_Date", StringType),
      StructField("_Class", LongType),
      StructField("_TagBased", BooleanType)
    ))

//   def apply_(implicit c: Catalog): Dep[_] =
//    "spark"
//      .as[SparkSession]
//      .map2("pathToBadgesFile".as[String]) { (spark, pathToBadgesFile) =>
//        logger.warn(s"Loading data from file: $pathToBadgesFile.")
//        spark
//          .readFromXmlFile(Schema, pathToBadgesFile)
//          .withColumnNamesNormalized
//          .select(
//            col("id"),
//            col("user_id"),
//            col("name"),
//            to_date(col("date").cast(TimestampType)).as("date"),
//            col("class"),
//            col("tag_based")
//          )
//      }
//      .as("badges")

  override implicit val id: String = "badges"

  override def apply(implicit c: Catalog): Dep[_] =
    Monad[Dep].map2("spark".as[SparkSession],  "pathToBadgesFile".as[String]  ) { (spark, pathToBadgesFile) =>
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
//      .as("badges")



}
