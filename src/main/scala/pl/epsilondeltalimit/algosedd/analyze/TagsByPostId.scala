package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object TagsByPostId extends Transformation {

  // TODO: this uses cartesian product !
  //    val tagsByPostId = posts.as("postsL")
  //      .join(posts.as("postsR"), when($"postsL.tags".isNotNull, $"postsL.id" === $"postsR.id").otherwise($"postsL.parent_id" === $"postsR.id"))
  //      .select(
  //        $"postsL.id".as("post_id"),
  //        $"postsR.tags".as("tags")
  //      )

  //    tagsByPostId.orderBy($"post_id".asc).show() //TODO: remove when implementation is finished

  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("tagsByQuestionPostId")
        .map2(c.get[DataFrame]("tagsByAnswerPostId")) {
          _ unionByName _
        }
        .as("tagsByPostId")
    }
}
