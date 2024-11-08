package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, isnull, not, size}
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.catalog.Catalog

object TagsByQuestionPostId extends (Catalog => Catalog) with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("posts")
        .map { posts =>
          logger.warn("Creating tags by post id map.")

          val postIsQuestion = col("post_type_id") === 1 &&
            not(isnull(col("tags"))) && size(col("tags")) =!= 0

          posts
            .where(postIsQuestion)
            .select(col("id").as("post_id"), col("tags"))
        }
        .as("tagsByQuestionPostId")
    }
}
