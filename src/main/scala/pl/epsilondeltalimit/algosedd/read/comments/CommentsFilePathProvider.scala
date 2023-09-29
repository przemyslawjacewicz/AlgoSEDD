package pl.epsilondeltalimit.algosedd.read.comments

import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object CommentsFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath").map("pathToCommentsFile") { rootPath =>
        s"$rootPath/Comments.xml"
      }
    }
}
