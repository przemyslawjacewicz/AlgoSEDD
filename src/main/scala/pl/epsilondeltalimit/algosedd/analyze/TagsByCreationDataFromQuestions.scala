package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.functions.{col, isnull, not}
import org.apache.spark.sql.{Column, DataFrame}
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.catalog.Catalog

object TagsByCreationDataFromQuestions extends (Catalog => Catalog) with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("posts")
        .map2(c.get[DataFrame]("tagsByPostId")) { (posts, tagsByPostId) =>
          logger.warn("Creating tags by creation date from questions.")
          val postRecordIsQuestion: Column = col("post_type_id") === 1 && not(isnull(col("tags")))

          posts
            .as("posts")
            .where(postRecordIsQuestion)
            .join(tagsByPostId.as("tagsByPostId"), col("posts.id") === col("tagsByPostId.post_id"))
            .select(col("creation_date"), col("tagsByPostId.tags"))
        }
        .as("tagsByCreationDataFromQuestions")
    }
}
