package pl.epsilondeltalimit.algosedd.read.users

import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object UsersFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath").map("pathToUsersFile") { rootPath =>
        s"$rootPath/Users.xml"
      }
    }

}
