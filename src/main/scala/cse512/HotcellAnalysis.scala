package cse512

import org.apache.avro.generic.GenericData.StringType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("xyz")
  val xyzCount = spark.sql("select x, y, z, count(*) as cnt from xyz where x >= " + minX + " and x <= " + maxX + " and y >= " + minY + " and y <= " + maxY + " and z >= " + minZ + " and z <= " + maxZ + " group by x, y, z")
  xyzCount.createOrReplaceTempView("xyzattr")
  val stat = spark.sql("select sum(cnt), sum(cnt * cnt) from xyzattr").first()
  //val cnt : Double = stat.get(2).toString.toDouble
  val xmean : Double = stat.get(0).toString.toDouble / numCells
  val xstddev : Double = Math.sqrt((stat.get(1).toString.toDouble / numCells) - xmean * xmean)
  // val A : Double = xmean * 27
  // val B : Double = xstddev * Math.sqrt(27)

  val joinDF = spark.sql("select xyz1.x as x1, xyz1.y as y1, xyz1.z as z1, xyz2.x as x2, xyz2.y as y2, xyz2.z as z2, xyz2.cnt as cnt from xyzattr as xyz1, xyzattr as xyz2 WHERE abs(xyz1.x - xyz2.x) <= 1 AND abs(xyz1.y - xyz2.y) <= 1 AND abs(xyz1.z - xyz2.z) <= 1")
  joinDF.createOrReplaceTempView("xyzjoin")
  spark.udf.register("sumOfWeight",(x: Integer, y: Integer, z:Integer)=>(
  HotcellUtils.sumOfWeight(x, y, z)
) )
  spark.udf.register("scaleValue",(x: Integer, y: Integer, z:Integer)=>(
    HotcellUtils.scaleValue(x, y, z)
    ) )
  // val aggDF = spark.sql("select x1, y1, z1, (sum(cnt) - " + A + ") / " + B + " from xyzjoin group by x1, y1, z1 order by sum(cnt) desc")
  val aggDF = spark.sql("select x1, y1, z1, (sum(cnt) - sumOfWeight(x1, y1, z1) * " + xmean + ") / " + xstddev + " /scaleValue(x1, y1, z1) as gvalue from xyzjoin group by x1, y1, z1 order by gvalue desc")
  return aggDF // YOU NEED TO CHANGE THIS PART
}
}
