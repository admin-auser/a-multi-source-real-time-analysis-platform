import org.apache.spark._
import java.io._
import scala.util.{Success, Try}
import scala.util.matching.Regex
object DataAll {
  def isIntByRegex(s : String) = {
    val pattern = """^(\d+)""".r
    s match {
      case pattern(_) => true
      case _ => false
    }
  }
  def verify(str: String, dtype: String):Boolean = {

    var c:Try[Any] = null
    if("double".equals(dtype)) {
      c = scala.util.Try(str.toDouble)
    }
    val result = c match {
      case Success(_) => true;
      case _ =>  false;
    }
    result
  }
  def main(args: Array[String]): Unit = {
    var province = "湖南省";
    var staName = "桔子洲";
    var parameter = "ph";
    var monthOrweek = "m";
    val conf = new SparkConf().setAppName("DataAll").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lines = sc.textFile("hdfs://localhost:9000/water-quality/dataset/all_result/all_data.csv",2)
    //val tmp = lines.filter(line => (line.trim().length > 0) && (line.split(",").length == 17) && (line.split(",")(0) == province))
    //val tmp = lines.filter(line => (line.trim().length > 0) && (line.split(",").length == 17) && isIntByRegex(line.split(",")(3)))
    val tmp = lines.filter(line => (line.trim().length > 0) && (line.split(",").length == 17))
    val writer = new PrintWriter(new File("/usr/local/water-quality/dataset/allDataAver.csv"))
    println("DataAll的结果：")
    var i = 0;
    val numList = List(5,6,7,10,11,12);
    for( i <- numList ){
      i match {
        case 5 => {
          parameter = "temp";
        }
        case 6 => {
          parameter = "ph";
        }
        case 7 => {
          parameter = "do";
        }
        case 10 => {
          parameter = "pp";
        }
        case 11 => {
          parameter = "an";
        }
        case 12 => {
          parameter = "tp";
        }
        case _ => {
          println("Not the desired parameter")
        }
      }
      println(i)
      monthOrweek = "m";
      val data = tmp.filter(x=>verify(x.split(",")(i), "double")).map(x=>{
           ((x.split(",")(2),x.split(",")(3).substring(0,7)),x.split(",")(i).toDouble)
      })
      val tmpresult = data.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1+y._1,x._2 + y._2)).mapValues(x => (x._1 / x._2).formatted("%.2f")).collect()
      val result = tmpresult.map(t => (t._1._1,t._1._2,parameter,monthOrweek,t._2).toString().replaceAll("\\(","").replaceAll("\\)",""))
      result.foreach(x => {
        println(x)
        writer.write(x+"\n")
      })
      monthOrweek = "y";
      val dataY = tmp.filter(x=>verify(x.split(",")(i), "double")).map(x=>{
           ((x.split(",")(2),x.split(",")(3).substring(0,4)),x.split(",")(i).toDouble)
      })
      val tmpresultY = dataY.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1+y._1,x._2 + y._2)).mapValues(x => (x._1 / x._2).formatted("%.2f")).collect()
      val resultY = tmpresultY.map(t => (t._1._1,t._1._2,parameter,monthOrweek,t._2).toString().replaceAll("\\(","").replaceAll("\\)",""))
      resultY.foreach(x => {
        println(x)
        writer.write(x+"\n")
      })
      /*monthOrweek = "w";
      val wdata = tmp.filter(x=>verify(x.split(",")(i), "double")).map(x=>{
           ((x.split(",")(2),x.split(",")(3)),x.split(",")(i).toDouble)
      })
      val timedata = wdata.map(x => ())*/
     //println(test)
      /*val tmpresult = data.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1+y._1,x._2 + y._2)).mapValues(x => (x._1 / x._2).formatted("%.2f")).collect()
      val result = tmpresult.map(t => (staName,t._1,parameter,monthOrweek,t._2).toString().replaceAll("\\(","").replaceAll("\\)",""))
      println(i)
      result.foreach(x => {
        println(x)
        writer.write(x+"\n")
      })*/
     }
   writer.close()
  }
}
