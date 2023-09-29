package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Dep, Transformation}

object TagsByCreationDataFromPostHistory extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("tagsByCreationDataFromPostHistory")(c.get[DataFrame]("postHistory"), c.get[DataFrame]("tagsByPostId")) { (postHistory, tagsByPostId) =>
        logger.warn("Creating tags by post creation date from post history.")
        postHistory
          .join(tagsByPostId, "post_id")
          .select(col("creation_date"), col("tags"))
      }
    }
}
