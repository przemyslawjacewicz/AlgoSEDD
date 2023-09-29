package pl.epsilondeltalimit.algosedd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import pl.epsilondeltalimit.dep.v6_1.Catalog

//TODO: customize logging - add start and finish msg
//TODO: optimize spark execution
object AlgoSEDD extends Logging {

  def main(args: Array[String]): Unit = {

    val catalog = new Catalog

    val startDate = args(0)
    catalog.unit("startDate")(startDate)

    val endDate = args(1)
    catalog.unit("endDate")(endDate)

    val aggregationInterval = args(2)
    catalog.unit("aggregationInterval")(aggregationInterval)

    val rootPath = args(3)
    catalog.unit("rootPath")(rootPath)

    val pathToOutput = args(4)
    catalog.unit("pathToOutput")(pathToOutput)

    //todo: remove me
    // NOTE: date filter applicable only for testing
    //    val dateFilter = $"creation_date" > "2017-12-31" && $"creation_date" < "2019-01-01"
    //    val dateFilter = $"creation_date" > "2019-05-31" && $"creation_date" < "2019-07-01"
    val dateFilter = lit(true)

    //    logger.warn("Reading data from dump files.")
    //    val pathToPostsFile = s"$rootPath/Posts.xml"
    //    val pathToTagsFile = s"$rootPath/Tags.xml"
    //    val pathToUsersFile = s"$rootPath/Users.xml"
    //    val pathToVotesFile = s"$rootPath/Votes.xml"

    //todo: convert to reflection-based transformation retrieval
    val transformations = Set(
      SparkSessionProvider,
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
      read.votes.VotesFileContentProvider,
      analyze.TagsByQuestionPostId,
      analyze.TagsByAnswerPostId,
      analyze.TagsByPostId,
      analyze.TagsByCreationDataFromQuestions,
      analyze.TagsByCreationDataFromAnswers,
      analyze.TagsByCreationDataFromComments,
      analyze.TagsByCreationDataFromVotes,
      analyze.TagsByCreationDataFromPostHistory,
      analyze.TagsByCreationDataFromPostLinks,
      analyze.DataEntriesCountByCreationDateAndTag

    )


    val output = transformations.foldLeft(catalog)((_c, _t) => _t(_c))

//    output.eval[DataFrame]("badges").show()
//    output.eval[DataFrame]("comments").show()
//    output.eval[DataFrame]("postHistory").show()
//    output.eval[DataFrame]("postLinks").show()
//    output.eval[DataFrame]("posts").show()
//    output.eval[DataFrame]("tags").show()
//    output.eval[DataFrame]("users").show()
//    output.eval[DataFrame]("votes").show()

    output.eval[DataFrame]("dataEntriesCountByCreationDateAndTag").show()

    /*
    //TODO: move to support
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val daysBetween = Duration.between(LocalDate.parse(startDate, fmt).atStartOfDay(), LocalDate.parse(endDate, fmt).atStartOfDay()).toDays
    val days = spark.createDataFrame(
      spark.sparkContext.parallelize(for (day <- 0L to daysBetween) yield RowFactory.create(Date.valueOf(LocalDate.parse(startDate, fmt).plusDays(day)))),
      StructType(Array(StructField("creation_date", DataTypes.DateType))))
    //    val days = (for (day <- 0L to daysBetween) yield LocalDate.parse(startDate, fmt).plusDays(day)).toDF("creation_date")

    val daysAndTags = days
      .crossJoin(tags.select($"tag_name".as("tag")))
    //    daysAndTags.show() //TODO: remove when implementation is finished

    logger.warn("Mapping entries to date range and tags.")
    val entriesCountByCreationDateAndTag = daysAndTags
      .join(dataEntriesCountByCreationDateAndTag, Seq("creation_date", "tag"), "left_outer")
    //    entriesCountByCreationDateAndTag.orderBy($"creation_date".desc).show() //TODO: remove when implementation is finished

    logger.warn("Aggregating entries by aggregation interval and tag.")
    val entriesCountByAggregationIntervalAndTag = entriesCountByCreationDateAndTag
      .groupBy(window($"creation_date", aggregationInterval).as("aggregation_interval"), $"tag")
      .agg(
        sum("q__entries_count_for_day_and_tag").as("q__entries_count_for_aggregation_interval_and_tag"),
        sum("a__entries_count_for_day_and_tag").as("a__entries_count_for_aggregation_interval_and_tag"),
        sum("c__entries_count_for_day_and_tag").as("c__entries_count_for_aggregation_interval_and_tag"),
        sum("v__entries_count_for_day_and_tag").as("v__entries_count_for_aggregation_interval_and_tag"),
        sum("ph__entries_count_for_day_and_tag").as("ph__entries_count_for_aggregation_interval_and_tag"),
        sum("pl__entries_count_for_day_and_tag").as("pl__entries_count_for_aggregation_interval_and_tag")
      )
    //    entriesCountByAggregationIntervalAndTag.orderBy($"aggregation_interval".desc).show() //TODO: remove when implementation is finished

    logger.warn("Aggregating entries by aggregation interval.")
    val entriesCountByAggregationInterval = entriesCountByAggregationIntervalAndTag
      .groupBy("aggregation_interval")
      .agg(
        sum($"q__entries_count_for_aggregation_interval_and_tag")
          .as("q__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag")
          .as("q_a__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag" +
          $"ph__entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v_ph__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag" +
          $"ph__entries_count_for_aggregation_interval_and_tag" +
          $"pl__entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v_ph_pl__entries_count_for_aggregation_interval")
      )
    //    entriesCountByAggregationInterval.orderBy($"aggregation_interval".desc).show() //TODO: remove when implementation is finished

    logger.warn("ANALYZING: {posts:questions | posts:answers | comments | votes | post_history | post_links}.")
    val entriesCount = entriesCountByAggregationIntervalAndTag
      .join(entriesCountByAggregationInterval, "aggregation_interval")
    //    entriesCount.orderBy($"aggregation_interval".desc) show() //TODO: remove when implementation is finished

    val relativePopularityByAggregationIntervalAndTag = entriesCount
      .withColumn("q__share",
        $"q__entries_count_for_aggregation_interval_and_tag" / $"q__entries_count_for_aggregation_interval")
      .withColumn("q_a__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag") / $"q_a__entries_count_for_aggregation_interval")
      .withColumn("q_a_c__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag") / $"q_a_c__entries_count_for_aggregation_interval")
      .withColumn("q_a_c_v__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v__entries_count_for_aggregation_interval")
      .withColumn("q_a_c_v_ph__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag" +
          $"ph__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v_ph__entries_count_for_aggregation_interval")
      .withColumn("q_a_c_v_ph_pl__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag" +
          $"ph__entries_count_for_aggregation_interval_and_tag" +
          $"pl__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v_ph_pl__entries_count_for_aggregation_interval")
      .withColumn("start", to_date($"aggregation_interval.start"))
      .withColumn("end", to_date($"aggregation_interval.end"))
      .drop("aggregation_interval")
    //    relativePopularityByAggregationIntervalAndTag.orderBy($"aggregation_interval".desc).show() //TODO: remove when implementation is finished

    logger.warn("Dumping relative popularity results.")
    relativePopularityByAggregationIntervalAndTag
      .na
      .fill(0)
      .select(
        $"start",
        $"end",
        $"tag",
        $"q__entries_count_for_aggregation_interval_and_tag",
        $"a__entries_count_for_aggregation_interval_and_tag",
        $"c__entries_count_for_aggregation_interval_and_tag",
        $"v__entries_count_for_aggregation_interval_and_tag",
        $"ph__entries_count_for_aggregation_interval_and_tag",
        $"pl__entries_count_for_aggregation_interval_and_tag",
        $"q__entries_count_for_aggregation_interval",
        $"q_a__entries_count_for_aggregation_interval",
        $"q_a_c__entries_count_for_aggregation_interval",
        $"q_a_c_v__entries_count_for_aggregation_interval",
        $"q_a_c_v_ph__entries_count_for_aggregation_interval",
        $"q_a_c_v_ph_pl__entries_count_for_aggregation_interval",
        $"q__share",
        $"q_a__share",
        $"q_a_c__share",
        $"q_a_c_v__share",
        $"q_a_c_v_ph__share",
        $"q_a_c_v_ph_pl__share",
      )
      .orderBy($"start".asc, $"tag".asc)
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .partitionBy("tag")
      .mode(SaveMode.Append)
      .save(pathToOutput)

    logger.warn("Dumping tags by entries count.")
    tags
      .orderBy($"count".desc)
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Append)
      .save(pathToOutput)

    logger.warn("Done. Exiting.")

     */
  }
}
