package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Dep, Transformation}

object TagsByCreationDataFromVotes extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("tagsByCreationDataFromVotes")(c.get[DataFrame]("votes"), c.get[DataFrame]("tagsByPostId")) { (votes, tagsByPostId) =>
        logger.warn("Creating tags by post creation date from votes.")
        votes
          .join(tagsByPostId, "post_id")
          .select(col("creation_date"), col("tags"))
      }
    }
}
