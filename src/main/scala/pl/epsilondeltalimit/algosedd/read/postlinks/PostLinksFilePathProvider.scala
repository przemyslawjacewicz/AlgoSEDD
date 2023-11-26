package pl.epsilondeltalimit.algosedd.read.postlinks

import pl.epsilondeltalimit.algosedd.read.FilePathProvider

object PostLinksFilePathProvider extends FilePathProvider {
  override val fileName: String = "PostLinks.xml"
  override val id: String       = "pathToPostLinksFile"
}
