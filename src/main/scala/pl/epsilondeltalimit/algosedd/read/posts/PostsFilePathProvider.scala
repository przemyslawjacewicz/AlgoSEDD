package pl.epsilondeltalimit.algosedd.read.posts

import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object PostsFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath")
        .map { rootPath =>
          s"$rootPath/Posts.xml"
        }
        .as("pathToPostsFile")
    }

}
