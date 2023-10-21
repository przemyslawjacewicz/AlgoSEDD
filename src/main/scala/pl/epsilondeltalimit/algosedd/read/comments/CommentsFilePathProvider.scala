package pl.epsilondeltalimit.algosedd.read.comments

import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object CommentsFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath")
        .map { rootPath =>
          s"$rootPath/Comments.xml"
        }
        .as("pathToCommentsFile")
    }
}
