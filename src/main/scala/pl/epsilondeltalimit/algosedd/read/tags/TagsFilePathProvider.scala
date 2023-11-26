package pl.epsilondeltalimit.algosedd.read.tags

import pl.epsilondeltalimit.algosedd.read.FilePathProvider

object TagsFilePathProvider extends FilePathProvider {
  override val fileName: String = "Tags.xml"
  override val id: String       = "pathToTagsFile"
}
