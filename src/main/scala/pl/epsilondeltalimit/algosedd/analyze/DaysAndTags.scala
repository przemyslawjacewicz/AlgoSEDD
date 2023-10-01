package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object DaysAndTags extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("days")
        .flatMap { days =>
          c.get[DataFrame]("tags").map { tags =>
            days
              .crossJoin(tags.select(col("tag_name").as("tag")))
          }
        }
        .map("daysAndTags")(identity)
    }
}
