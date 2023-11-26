package pl.epsilondeltalimit.algosedd.read.posts

import pl.epsilondeltalimit.algosedd.read.FilePathProvider

object PostsFilePathProvider extends FilePathProvider {
  override val fileName: String = "Posts.xml"
  override val id: String       = "pathToPostsFile"
}
