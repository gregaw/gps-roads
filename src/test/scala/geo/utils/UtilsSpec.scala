package geo.utils

import org.scalatest.FlatSpec

class UtilsSpec extends FlatSpec with GeoSparkTestWrapper {

  import geo.utils.GeoSpark.Implicits._

  "GeoNumber " should " convert from meters ok " in {
    assert(Utils.meterLatitudeFactor === 1.meter)
    assert(1.meter === 1.m)
    assert(1.meter === 1.meters)
    assert(2.m === 2 * 1.m)
    assert(1.km === 1000.m)
    assert(0.1.meter === 0.1 * 1.meter)
  }

  "Rich Coordinate " should " shift corrdinates beautifully" in {
    assert(vavel.shift(x = 1.meter).x === vavel.x + 1.meter)
    assert(vavel.shift(y = 1.meter).y === vavel.y + 1.meter)
  }

  it should "understand norths and wests " in {
    assert(vavel.west(1.km) === vavel.shift(x = -1.km))
    assert(vavel.east(1.km) === vavel.shift(x = 1.km))
    assert(vavel.north(1.km) === vavel.shift(y = 1.km))
    assert(vavel.south(1.km) === vavel.shift(y = -1.km))

  }


}
