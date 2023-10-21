package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.types._
import org.apache.spark.sql.{RowFactory, SparkSession}
import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

import java.sql.Date
import java.time._

object Days extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[SparkSession]("spark")
        .flatMap { spark =>
          c.get[LocalDate]("startDate").flatMap { startDate =>
            c.get[LocalDate]("endDate").map { endDate =>
              val daysBetween = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays
              spark.createDataFrame(
                spark.sparkContext.parallelize(
                  for (day <- 0L to daysBetween) yield RowFactory.create(Date.valueOf(startDate.plusDays(day)))),
                StructType(Array(StructField("creation_date", DataTypes.DateType)))
              )
            //              (for (day <- 0L to daysBetween) yield LocalDate.parse(startDate, fmt).plusDays(day)).toDF("creation_date")
            }
          }
        }
        .as("days")
    }
}
