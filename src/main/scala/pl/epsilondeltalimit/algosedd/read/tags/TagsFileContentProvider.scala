package pl.epsilondeltalimit.algosedd.read.tags

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object TagsFileContentProvider extends PutTransformationWithImplicitCatalog with Logging {
  import Dep.implicits._

  private val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_TagName", StringType),
      StructField("_Count", LongType),
      StructField("_ExcerptPostId", LongType),
      StructField("_WikiPostId", LongType)
    ))

  override def apply(implicit c: Catalog): Dep[_] =
    "spark"
      .as[SparkSession]
      .map2("pathToTagsFile".as[String]) { (spark, pathToTagsFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToTagsFile.")

        spark
          .readFromXmlFile(Schema, pathToTagsFile)
          .withColumnNamesNormalized
          .select($"id", $"tag_name", $"count", $"excerpt_post_id", $"wiki_post_id")
      }
      .as("tags")
}
