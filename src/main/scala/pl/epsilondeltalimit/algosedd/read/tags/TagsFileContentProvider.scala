package pl.epsilondeltalimit.algosedd.read.tags

import cats.Monad
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.implicits._
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Dep
import pl.epsilondeltalimit.dep.transformation.DepTransformationImplicit

object TagsFileContentProvider extends DepTransformationImplicit[DataFrame] with Logging {

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_TagName", StringType),
      StructField("_Count", LongType),
      StructField("_ExcerptPostId", LongType),
      StructField("_WikiPostId", LongType)
    ))

  override def apply(implicit c: Catalog): Dep[DataFrame] =
    Monad[Dep]
      .map2("spark".as[SparkSession], "pathToTagsFile".as[String]) { (spark, pathToTagsFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToTagsFile.")

        spark
          .readFromXmlFile(Schema, pathToTagsFile)
          .withColumnNamesNormalized
          .select($"id", $"tag_name", $"count", $"excerpt_post_id", $"wiki_post_id")
      }
      .as("tags")

}
