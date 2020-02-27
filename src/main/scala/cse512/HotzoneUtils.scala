package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    var rect = queryRectangle.split(',').map(_.toDouble)
    var point = pointString.split(',').map(_.toDouble)
    return point(0) >= rect(0) && point(0) <= rect(2) && point(1) >= rect(1) && point(1) <= rect(3) ||
      point(0) >= rect(2) && point(0) <= rect(0) && point(1) >= rect(3) && point(1) <= rect(1)
  }

  // YOU NEED TO CHANGE THIS PART

}
