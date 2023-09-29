package pl.epsilondeltalimit.algosedd.read.posthistory

import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object PostHistoryFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath").map("pathToPostHistoryFile") { rootPath =>
        s"$rootPath/PostHistory.xml"
      }
    }

}
