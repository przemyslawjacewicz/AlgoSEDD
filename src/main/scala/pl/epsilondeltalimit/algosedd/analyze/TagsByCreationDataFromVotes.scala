package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object TagsByCreationDataFromVotes extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("votes")
        .map2(c.get[DataFrame]("tagsByPostId")) { (votes, tagsByPostId) =>
          logger.warn("Creating tags by post creation date from votes.")
          votes
            .join(tagsByPostId, "post_id")
            .select(col("creation_date"), col("tags"))
        }
        .as("tagsByCreationDataFromVotes")
    }
}
