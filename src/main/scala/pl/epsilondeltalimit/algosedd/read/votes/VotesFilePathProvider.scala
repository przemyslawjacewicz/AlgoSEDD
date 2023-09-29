package pl.epsilondeltalimit.algosedd.read.votes

import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object VotesFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath").map("pathToVotesFile") { rootPath =>
        s"$rootPath/Votes.xml"
      }
    }

}
