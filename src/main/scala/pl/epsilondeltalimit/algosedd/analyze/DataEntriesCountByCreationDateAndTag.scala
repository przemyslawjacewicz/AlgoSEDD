package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object DataEntriesCountByCreationDateAndTag extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      logger.warn("Counting entries by creation date and tag.")

      val creationDateAndTag = Seq("creation_date", "tag")
      c.get[DataFrame]("tagsByCreationDataFromQuestions")
        .flatMap(tagsByCreationDataFromQuestions =>
          c.get[DataFrame]("tagsByCreationDataFromAnswers")
            .flatMap(tagsByCreationDataFromAnswers =>
              c.get[DataFrame]("tagsByCreationDataFromComments")
                .flatMap(tagsByCreationDataFromComments =>
                  c.get[DataFrame]("tagsByCreationDataFromVotes")
                    .flatMap(tagsByCreationDataFromVotes =>
                      c.get[DataFrame]("tagsByCreationDataFromPostHistory")
                        .flatMap(tagsByCreationDataFromPostHistory =>
                          c.get[DataFrame]("tagsByCreationDataFromPostLinks")
                            .map(tagsByCreationDataFromPostLinks =>
                              countEntriesByCreationDateAndTag(tagsByCreationDataFromQuestions)
                                .withColumnRenamed("count", "q__entries_count_for_day_and_tag")
                                .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromAnswers),
                                      creationDateAndTag)
                                .withColumnRenamed("count", "a__entries_count_for_day_and_tag")
                                .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromComments),
                                      creationDateAndTag)
                                .withColumnRenamed("count", "c__entries_count_for_day_and_tag")
                                .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromVotes), creationDateAndTag)
                                .withColumnRenamed("count", "v__entries_count_for_day_and_tag")
                                .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromPostHistory),
                                      creationDateAndTag)
                                .withColumnRenamed("count", "ph__entries_count_for_day_and_tag")
                                .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromPostLinks),
                                      creationDateAndTag)
                                .withColumnRenamed("count", "pl__entries_count_for_day_and_tag")))))))
        .as("dataEntriesCountByCreationDateAndTag")
    }
}
