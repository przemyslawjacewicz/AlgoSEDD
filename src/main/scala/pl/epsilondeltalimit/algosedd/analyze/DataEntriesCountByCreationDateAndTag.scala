package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.Dep.implicits._
import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object DataEntriesCountByCreationDateAndTag extends PutTransformationWithImplicitCatalog with Logging {

  override def apply(implicit c: Catalog): Dep[_] = {
    logger.warn("Counting entries by creation date and tag.")

    val creationDateAndTag = Seq("creation_date", "tag")

    (for {
      tagsByCreationDataFromQuestions <- "tagsByCreationDataFromQuestions".as[DataFrame]
      tagsByCreationDataFromAnswers <- "tagsByCreationDataFromAnswers".as[DataFrame]
      tagsByCreationDataFromComments <- "tagsByCreationDataFromComments".as[DataFrame]
      tagsByCreationDataFromVotes <- "tagsByCreationDataFromVotes".as[DataFrame]
      tagsByCreationDataFromPostHistory <- "tagsByCreationDataFromPostHistory".as[DataFrame]
      tagsByCreationDataFromPostLinks <- "tagsByCreationDataFromPostLinks".as[DataFrame]
    } yield
      countEntriesByCreationDateAndTag(tagsByCreationDataFromQuestions)
        .withColumnRenamed("count", "q__entries_count_for_day_and_tag")
        .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromAnswers), creationDateAndTag)
        .withColumnRenamed("count", "a__entries_count_for_day_and_tag")
        .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromComments), creationDateAndTag)
        .withColumnRenamed("count", "c__entries_count_for_day_and_tag")
        .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromVotes), creationDateAndTag)
        .withColumnRenamed("count", "v__entries_count_for_day_and_tag")
        .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromPostHistory), creationDateAndTag)
        .withColumnRenamed("count", "ph__entries_count_for_day_and_tag")
        .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromPostLinks), creationDateAndTag)
        .withColumnRenamed("count", "pl__entries_count_for_day_and_tag"))
      .as("dataEntriesCountByCreationDateAndTag")
  }
}
