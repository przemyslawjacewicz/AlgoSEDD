package pl.epsilondeltalimit.algosedd.write

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.catalog.Catalog

object TagsStorage extends (Catalog => Catalog) with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("tags")
        .flatMap { tags =>
          c.get[String]("pathToOutput").map { pathToOutput =>
            logger.warn("Dumping tags by entries count.")
            tags
              .orderBy(col("count").desc)
              .coalesce(1)
              .write
              .format("csv")
              .option("header", "true")
              .mode(SaveMode.Append)
              .save(pathToOutput)
          }
        }
        .as("tagsStorage")
    }
}
