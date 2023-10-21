package pl.epsilondeltalimit.algosedd.read.tags

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.Transformations.Transformation
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object TagsFileContentProvider extends Transformation with Logging {

  private[this] val Schema: StructType = StructType(
    Array(
      StructField("_Id", LongType),
      StructField("_TagName", StringType),
      StructField("_Count", LongType),
      StructField("_ExcerptPostId", LongType),
      StructField("_WikiPostId", LongType)
    ))

  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("tags")(c.get[SparkSession]("spark"), c.get[String]("pathToTagsFile")) { (spark, pathToTagsFile) =>
        import spark.implicits._

        logger.warn(s"Loading data from file: $pathToTagsFile.")

        spark
          .readFromXmlFile(Schema, pathToTagsFile)
          .withColumnNamesNormalized
          .select($"id", $"tag_name", $"count", $"excerpt_post_id", $"wiki_post_id")
      }
    }

}
