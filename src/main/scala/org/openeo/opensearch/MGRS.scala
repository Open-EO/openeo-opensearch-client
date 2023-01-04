package org.openeo.opensearch

import geotrellis.vector.Extent

import scala.math.pow

object MGRS {

  def mgrsToSentinel2Extent(mgrsString:String): Extent = {
    val decoded = decode(mgrsString)
    val easting = decoded._1._1
    val north = decoded._1._4 >= 'N'
    val southing = decoded._1._2

    val diff = easting % 300000
    val eastingS2:Double =
      if(diff==100000) {
        easting-40
      }else if(diff==200000) {
        easting-20
      }else{
        easting
      }
    val ydiff = southing % 300000
    val southingS2:Double =
      if(ydiff==100000) {
        southing-( if(north) 9760 else 9780 )
      }else if(ydiff==200000) {
        southing - (if(north) 9800 else 9760)
      }else{
        southing - (if(north) 9780 else 9800 )
      }

    new Extent(eastingS2,southingS2,eastingS2+109800,southingS2+109800)
  }

  def decode(mgrsString: String): ((Int, Int, Int, Char), Double) = {

    if (mgrsString.length == 0) {
      throw new IllegalArgumentException("Cannot generate MGRS from empty string")
    }

    val length = mgrsString.length

    var sb = ""
    var i = 0

    // get Zone number
    while (! Range('A', 'Z', 1).contains(mgrsString.charAt(i))) {
      if (i >= 2) {
        throw new IllegalArgumentException (s"Bad MGRS conversion from $mgrsString")
      }
      sb += mgrsString.charAt(i)
      i += 1
    }

    if (i == 0 || i + 3 > length) {
      // A good MGRS string has to be 4-5 digits long,
      // ##AAA/#AAA at least.
      throw new IllegalArgumentException (s"Bad MGRS conversion from $mgrsString")
    }

    val zoneNumber = sb.toInt
    val zoneLetter = mgrsString.charAt(i)

    // Should we check the zone letter here? Why not.
    if (zoneLetter <= 'B' || zoneLetter >= 'Y' || zoneLetter == 'I' || zoneLetter == 'O') {
      throw new IllegalArgumentException(s"MGRS zone letter $zoneLetter not allowed in $mgrsString")
    }

    i += 1
    val hunK = mgrsString.drop(i).take(2)
    var set = get100kSetForZone(zoneNumber)
    i += 2

    var east100k = getEastingFromChar(hunK.charAt(0), set)
    var north100k = getNorthingFromChar(hunK.charAt(1), set)

    // We have a bug where the northing may be 2000000 too low.
    // How do we know when to roll over?
    while (north100k < getMinNorthing(zoneLetter)) {
      north100k += 2000000
    }

    // calculate the char index for easting/northing separator
    val remainder = length - i

    if (remainder % 2 != 0) {
      throw new IllegalArgumentException("MGRS must have an even number of digits after the zone letter/100km letters in $mgrsString")
    }

    val sep = remainder / 2

    val (sepEasting, sepNorthing, accuracyBonus) =
      if (sep > 0) {
        val aB = 100000.0 / pow(10, sep)
        val sepEastingString = mgrsString.substring(i, i + sep)
        val sepNorthingString = mgrsString.substring(i + sep)
        (sepEastingString.toDouble * aB, sepNorthingString.toDouble * aB, aB)
      } else {
        (0.0, 0.0, 0.0)
      }

    val easting = sepEasting + east100k
    val northing = sepNorthing + north100k

    ((easting.toInt, northing.toInt, zoneNumber, zoneLetter), accuracyBonus)
  }

  private val NUM_100K_SETS = 6
  private val SET_ORIGIN_COLUMN_LETTERS = "AJSAJS"
  private val SET_ORIGIN_ROW_LETTERS = "AFAFAF"

  private val _A = 65
  private val _I = 73
  private val _O = 79
  private val _V = 86
  private val _Z = 90

  private def get100kSetForZone(i: Int) = {
    val setParm = i % NUM_100K_SETS
    if (setParm == 0)
      NUM_100K_SETS
    else
      setParm
  }

  private def getEastingFromChar(e: Char, set: Int): Double = {
    // colOrigin is the letter at the origin of the set for the column
    var curCol = SET_ORIGIN_COLUMN_LETTERS.charAt(set - 1).toInt
    var eastingValue = 100000.0
    var rewindMarker = false

    while (curCol != e.toInt) {
      curCol += 1
      if (curCol == _I) {
        curCol += 1
      }
      if (curCol == _O) {
        curCol += 1
      }
      if (curCol > _Z) {
        if (rewindMarker) {
          throw new IllegalArgumentException("Bad character $e in getEastingFromChar")
        }
        curCol = _A
        rewindMarker = true
      }
      eastingValue += 100000.0
    }

    eastingValue
  }

  /**
   * Given the second letter from a two-letter MGRS 100k zone, and given the
   * MGRS table set for the zone number, figure out the northing value that
   * should be added to the other, secondary northing value. You have to
   * remember that Northings are determined from the equator, and the vertical
   * cycle of letters mean a 2000000 additional northing meters. This happens
   * approx. every 18 degrees of latitude. This method does *NOT* count any
   * additional northings. You have to figure out how many 2000000 meters need
   * to be added for the zone letter of the MGRS coordinate.
   *
   * n   : Second letter of the MGRS 100k zone
   * set : The MGRS table set number, which is dependent on the UTM zone number.
   *
   * Returns the northing value for the given letter and set.
   */
  private def getNorthingFromChar(n: Char, set: Int): Double = {

    if (n > 'V') {
      throw new IllegalArgumentException("Invalid northing, $n, passed to getNorthingFromChar")
    }

    // rowOrigin is the letter at the origin of the set for the column
    var curRow = SET_ORIGIN_ROW_LETTERS.charAt(set - 1).toInt
    var northingValue = 0.0
    var rewindMarker = false

    while (curRow != n.toInt) {
      curRow += 1
      if (curRow == _I) {
        curRow += 1
      }
      if (curRow == _O) {
        curRow += 1
      }
      // fixing a bug making whole application hang in this loop
      // when 'n' is a wrong character
      if (curRow > _V) {
        if (rewindMarker) { // making sure that this loop ends
          throw new IllegalArgumentException("Bad character, $n, passed to getNorthingFromChar")
        }
        curRow = _A
        rewindMarker = true
      }
      northingValue += 100000.0
    }

    northingValue
  }

  /**
   * The function getMinNorthing returns the minimum northing value of a MGRS
   * zone.
   *
   * Ported from Geotrans' c Lattitude_Band_Value structure table.
   *
   * zoneLetter : The MGRS zone to get the min northing for.
   */
  private def getMinNorthing(zoneLetter: Char): Double = {
    zoneLetter match {
      case 'C' => 1100000.0
      case 'D' => 2000000.0
      case 'E' => 2800000.0
      case 'F' => 3700000.0
      case 'G' => 4600000.0
      case 'H' => 5500000.0
      case 'J' => 6400000.0
      case 'K' => 7300000.0
      case 'L' => 8200000.0
      case 'M' => 9100000.0
      case 'N' => 0.0
      case 'P' => 800000.0
      case 'Q' => 1700000.0
      case 'R' => 2600000.0
      case 'S' => 3500000.0
      case 'T' => 4400000.0
      case 'U' => 5300000.0
      case 'V' => 6200000.0
      case 'W' => 7000000.0
      case 'X' => 7900000.0
      case _ => throw new IllegalArgumentException(s"Invalid zone letter, $zoneLetter")
    }
  }
}
