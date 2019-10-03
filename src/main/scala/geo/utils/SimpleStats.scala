package geo.utils

import scala.collection.mutable

/**
  * Collects series of values per category, calculates statistics and produces report.
  *
  * {{{
  * scala> val stats = new SimpleStats("prefixHead")
  * for (i <- 1 to 10) {
  *   stats.collect("csv-Prefix,act--A", i)
  *   stats.collect("csv-Prefix,act--B", i*2)
  * }
  * stats.report
  *
  * stats: geo.utils.SimpleStats = geo.utils.SimpleStats@32328dc4
  *
  * scala>      |      |      |
  * scala> res7: String =
  * "
  * prefixHead,action,   mean>2,    std>2,     mean,      std,      min,      max,    count
  * csv-Prefix,act--A,    6.500,    2.291,    5.500,    2.872,    1.000,   10.000,   10.000
  * csv-Prefix,act--B,   13.000,    4.583,   11.000,    5.745,    2.000,   20.000,   10.000
  * "
  * }}}
  *
  * Note:
  * - mean>2 = mean(values.drop(2))
  * - std>2 = stdDev(values.drop(2))
  */
class SimpleStats(headerCsvPrefix: String) {

  import GeoSpark.Implicits._

  private val stats = mutable.Map[String, mutable.ArrayBuffer[Double]]().withDefault(_ => new mutable.ArrayBuffer[Double])

  def collect(key: String, value: Double): Unit = {
    val ar = stats(key)
    ar.append(value)
    stats(key) = ar
  }

  def report: String = {
    val longest_key_length = stats.keys.map(_.length).max
    val tuples = stats.map { case (name, values) =>
      val computed =
        Vector(
          mean(values.drop(2)),
          stdDev(values.drop(2)),
          mean(values),
          stdDev(values),
          values.min,
          values.max,
          values.length
        )
          .map(value => f"$value%8.3f")

      (name.padRight(longest_key_length) +: computed).mkString(", ")
    }.toVector

    "\n" +
      s"$headerCsvPrefix,action".padRight(longest_key_length) +
      "," +
      "mean>2,std>2,mean,std,min,max,count".split(",").map(_.padLeft(9)).mkString(",") +
      "\n" +
      tuples.sorted.mkString("\n") +
      "\n"
  }


  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

}
