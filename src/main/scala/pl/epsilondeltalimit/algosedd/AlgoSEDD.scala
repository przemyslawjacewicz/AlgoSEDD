package pl.epsilondeltalimit.algosedd

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.catalog.untyped.UntypedCatalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.implicits._

import java.time.LocalDate

//TODO: customize logging - add start and finish msg
object AlgoSEDD extends Logging {

  def main(args: Array[String]): Unit = {
    val startDate           = args(0)
    val endDate             = args(1)
    val aggregationInterval = args(2)
    val rootPath            = args(3)
    val pathToOutput        = args(4)

    // todo: remove me
    // NOTE: date filter applicable only for testing
    //    val dateFilter = $"creation_date" > "2017-12-31" && $"creation_date" < "2019-01-01"
    //    val dateFilter = $"creation_date" > "2019-05-31" && $"creation_date" < "2019-07-01"
//    val dateFilter = lit(true)

    val spark: Seq[Catalog => Catalog] = Seq(
      SparkSessionProvider
    )

    val readers: Seq[Catalog => Result[_]] = Seq(
      read.badges.BadgesFilePathProvider,
      read.badges.BadgesFileContentProvider,
      read.comments.CommentsFilePathProvider,
      read.comments.CommentsFileContentProvider,
      read.posthistory.PostHistoryFilePathProvider,
      read.posthistory.PostHistoryFileContentProvider,
      read.postlinks.PostLinksFilePathProvider,
      read.postlinks.PostLinksFileContentProvider,
      read.posts.PostsFilePathProvider,
      read.posts.PostsFileContentProvider,
      read.tags.TagsFilePathProvider,
      read.tags.TagsFileContentProvider,
      read.users.UsersFilePathProvider,
      read.users.UsersFileContentProvider,
      read.votes.VotesFilePathProvider,
      read.votes.VotesFileContentProvider
    )

    val analyzers: Seq[Catalog => Catalog] = Seq(
      analyze.TagsByQuestionPostId,
      analyze.TagsByAnswerPostId,
      analyze.TagsByPostId,
      analyze.TagsByCreationDataFromQuestions,
      analyze.TagsByCreationDataFromAnswers,
      analyze.TagsByCreationDataFromComments,
      analyze.TagsByCreationDataFromVotes,
      analyze.TagsByCreationDataFromPostHistory,
      analyze.TagsByCreationDataFromPostLinks,
      analyze.DataEntriesCountByCreationDateAndTag,
      analyze.Days,
      analyze.DaysAndTags,
      analyze.EntriesCountByCreationDateAndTag,
      analyze.EntriesCountByAggregationIntervalAndTag,
      analyze.EntriesCountByAggregationInterval,
      analyze.EntriesCount,
      analyze.RelativePopularityByAggregationIntervalAndTag
    )

    val writers: Seq[Catalog => Catalog] = Seq(
      write.RelativePopularityByAggregationIntervalAndTagStorage,
      write.TagsStorage
    )

    val output = (new UntypedCatalog)
      .put("startDate")(LocalDate.parse(startDate))
      .put("endDate")(LocalDate.parse(endDate))
      .put("aggregationInterval")(aggregationInterval)
      .put("rootPath")(rootPath)
      .put("pathToOutput")(pathToOutput)
      .withTransformations(spark: _*)
      .withTransformations(readers: _*)
      .withTransformations(analyzers: _*)
      .withTransformations(writers: _*)

    println(output.explain("relativePopularityByAggregationIntervalAndTag"))
    output.eval[DataFrame]("relativePopularityByAggregationIntervalAndTag").show()

    logger.warn("Done. Exiting.")
  }
}
