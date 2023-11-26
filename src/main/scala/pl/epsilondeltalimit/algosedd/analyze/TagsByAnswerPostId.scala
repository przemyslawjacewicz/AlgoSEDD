package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object TagsByAnswerPostId extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("posts")
        .map2(c.get[DataFrame]("tagsByQuestionPostId")) { (posts, tagsByQuestionPostId) =>
          logger.warn("Creating tags by post id map.")

          val postIsAnswer = col("post_type_id") === 2 && not(isnull(col("parent_id")))
          posts
            .as("posts")
            .where(postIsAnswer)
            .join(
              tagsByQuestionPostId.alias("tagsByQuestionPostId"),
              col("posts.parent_id") === col("tagsByQuestionPostId.post_id")
            )
            .select(col("id").as("post_id"), col("tagsByQuestionPostId.tags"))
        }
        .as("tagsByAnswerPostId")
    }
}
