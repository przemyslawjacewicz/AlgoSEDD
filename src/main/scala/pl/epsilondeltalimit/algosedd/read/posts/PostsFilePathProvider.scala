package pl.epsilondeltalimit.algosedd.read.posts

import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object PostsFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath").map("pathToPostsFile") { rootPath =>
        s"$rootPath/Posts.xml"
      }
    }

}
