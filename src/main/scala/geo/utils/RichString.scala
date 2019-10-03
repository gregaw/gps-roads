package geo.utils

class RichString(value: String) {

  def padRight(count: Int, filler: Char = ' '): String = value.padTo(count, filler).mkString

  def padLeft(count: Int, filler: Char = ' '): String = value.reverse.padTo(count, filler).reverse.mkString
}

