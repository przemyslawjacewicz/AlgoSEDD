package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object EntriesCountByCreationDateAndTag extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("daysAndTags")
        .flatMap { daysAndTags =>
          c.get[DataFrame]("dataEntriesCountByCreationDateAndTag").map { dataEntriesCountByCreationDateAndTag =>
            logger.warn("Mapping entries to date range and tags.")
            daysAndTags
              .join(dataEntriesCountByCreationDateAndTag, Seq("creation_date", "tag"), "left_outer")
          }
        }
        .map("entriesCountByCreationDateAndTag")(identity)
    }
}
