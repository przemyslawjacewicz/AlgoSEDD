package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.functions.{col, isnull, not}
import org.apache.spark.sql.{Column, DataFrame}
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Dep, Transformation}

object TagsByCreationDataFromQuestions extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("tagsByCreationDataFromQuestions")(c.get[DataFrame]("posts"), c.get[DataFrame]("tagsByPostId")) { (posts, tagsByPostId) =>
        logger.warn("Creating tags by creation date from questions.")
        val postRecordIsQuestion: Column = col("post_type_id") === 1 && not(isnull(col("tags")))

        posts.as("posts")
          .where(postRecordIsQuestion)
          .join(tagsByPostId.as("tagsByPostId"), col("posts.id") === col("tagsByPostId.post_id"))
          .select(col("creation_date"), col("tagsByPostId.tags"))
      }
    }
}
