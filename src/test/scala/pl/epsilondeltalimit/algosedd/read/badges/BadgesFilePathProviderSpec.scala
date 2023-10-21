package pl.epsilondeltalimit.algosedd.read.badges

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pl.epsilondeltalimit.dep.Catalog

class BadgesFilePathProviderSpec extends AnyFlatSpec with Matchers {

  "apply" should "return file path" in {
    val c = (new Catalog).unit("rootPath")("/path/to/root")

    BadgesFilePathProvider(c).eval[String]("pathToBadgesFile") should ===("/path/to/root/Badges.xml")
  }

}
