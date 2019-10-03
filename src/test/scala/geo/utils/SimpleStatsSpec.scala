package geo.utils

import org.scalatest.{FlatSpec, Matchers}

class SimpleStatsSpec extends FlatSpec with Matchers {

  "Stats" should "generate a nice report" in {
    val s = new SimpleStats(headerCsvPrefix = "prefixCsv")

    for (i<-1 to 3) {
      s.collect("key1", i)
      s.collect("longer_key2", i)
    }

    assert(s.report ===
"""
  |prefixCsv,action,   mean>2,    std>2,     mean,      std,      min,      max,    count
  |key1       ,    3.000,    0.000,    2.000,    0.816,    1.000,    3.000,    3.000
  |longer_key2,    3.000,    0.000,    2.000,    0.816,    1.000,    3.000,    3.000
  |""".stripMargin)

  }

}
