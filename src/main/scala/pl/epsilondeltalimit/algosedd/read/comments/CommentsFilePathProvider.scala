package pl.epsilondeltalimit.algosedd.read.comments

import pl.epsilondeltalimit.algosedd.read.FilePathProvider

object CommentsFilePathProvider extends FilePathProvider {
  override val fileName: String = "Comments.xml"
  override val id: String       = "pathToCommentsFile"
}
