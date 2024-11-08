package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.catalog.Catalog

object TagsByCreationDataFromPostHistory extends (Catalog => Catalog) with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("postHistory")
        .map2(c.get[DataFrame]("tagsByPostId")) { (postHistory, tagsByPostId) =>
          logger.warn("Creating tags by post creation date from post history.")
          postHistory
            .join(tagsByPostId, "post_id")
            .select(col("creation_date"), col("tags"))
        }
        .as("tagsByCreationDataFromPostHistory")
    }
}
