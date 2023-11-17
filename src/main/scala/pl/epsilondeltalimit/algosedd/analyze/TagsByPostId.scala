package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.dep.Transformations.Transformation
import pl.epsilondeltalimit.dep.{Catalog, Dep}

import cats._
import cats.implicits._

object TagsByPostId extends Transformation {
import pl.epsilondeltalimit.algosedd._

  // TODO: this uses cartesian product !
  //    val tagsByPostId = posts.as("postsL")
  //      .join(posts.as("postsR"), when($"postsL.tags".isNotNull, $"postsL.id" === $"postsR.id").otherwise($"postsL.parent_id" === $"postsR.id"))
  //      .select(
  //        $"postsL.id".as("post_id"),
  //        $"postsR.tags".as("tags")
  //      )

  //    tagsByPostId.orderBy($"post_id".asc).show() //TODO: remove when implementation is finished

  override def apply(c: Catalog): Catalog = {
//    c.put {
//      Dep.map2("tagsByPostId")(c.get[DataFrame]("tagsByQuestionPostId"), c.get[DataFrame]("tagsByAnswerPostId")) {
//        _ unionByName _
//      }
//    }

    implicit val id: String = "tagsByPostId"

    c.put{
      Monad[Dep].map2(c.get[DataFrame]("tagsByQuestionPostId"), c.get[DataFrame]("tagsByAnswerPostId")) {
        _ unionByName _
      }
    }





  }
}
