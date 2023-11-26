package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object DataEntriesCountByCreationDateAndTag extends Transformation with Logging {

  val creationDateAndTag: Seq[String] = Seq("creation_date", "tag")

  override def apply(c: Catalog): Catalog =
    c.put {
      {
        for {
          tagsByCreationDataFromQuestions   <- c.get[DataFrame]("tagsByCreationDataFromQuestions")
          tagsByCreationDataFromAnswers     <- c.get[DataFrame]("tagsByCreationDataFromAnswers")
          tagsByCreationDataFromComments    <- c.get[DataFrame]("tagsByCreationDataFromComments")
          tagsByCreationDataFromVotes       <- c.get[DataFrame]("tagsByCreationDataFromVotes")
          tagsByCreationDataFromPostHistory <- c.get[DataFrame]("tagsByCreationDataFromPostHistory")
          tagsByCreationDataFromPostLinks   <- c.get[DataFrame]("tagsByCreationDataFromPostLinks")
        } yield {
          logger.warn("Counting entries by creation date and tag.")

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
            .withColumnRenamed("count", "pl__entries_count_for_day_and_tag")
        }
      }
        .as("dataEntriesCountByCreationDateAndTag")
    }
}
