package geo.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geo.Params

case class RunContext(params: Params, testName: String, iteration: Int) {
  val keyCsv = s"${RunContext.execId},${params.runSummaryCsv},$testName"
}

object RunContext {
  lazy val execId = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
  lazy val headerCsv = s"execId,${Params.runSummaryHeaderCsv},testName"
}
