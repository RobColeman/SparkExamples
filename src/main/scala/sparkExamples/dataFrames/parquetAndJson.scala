package sparkExamples.dataFrames

import java.io.File
import java.lang.System._
import org.apache.commons.io.FileUtils

import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object EventsAsJson {

  def main(args: Array[String]): Unit = {

    val startTime = (currentTimeMillis() / 1000.0).toInt

    val appName = "decision-stumps-one-test"
    val conf = new SparkConf().setAppName(appName).setMaster("local[16]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    //create some data in an RDD.  The data items are the Event class below
    val eventsRDD: RDD[Event] = EventHelper.createRDDofFakeEvents(sc).cache()

    /*
    to convert the RDD[T] to a DataFrame you must either
      use the sqlContext.createDataFrame method
      or import all of the sqlContext implicit methods for RDDs
     */
    import sqlContext.implicits._

    // then convert the RDD[T] to a DataFrame,
    // if you've imported the sqlContext implicits, then use the .toDF method
    val eventsDF: DataFrame = eventsRDD.toDF()


    //DF.toJSON will convert your DF to properly formatted JSON
    val eventsAsJson = eventsDF.toJSON
    eventsAsJson.take(10).foreach(println)

    /*
    writing properly formatted JSON is as easy as calling
      DF.write.json(<path>)

    this will generate nPartitions PART-XXXX json files under the folder <path>, paried with some schema files

    this will work with any filesystem that spark can write to.
    such as:
      Hadoop
      Amazon s3
      Tachyon
      Local filesystem
    */
    val path = "/tmp/jsonTest"
    FileUtils.deleteQuietly(new File(path))
    eventsDF.write.json(path)

    /*
    loading in the JSON data is as easy as calling
    sqlContext.read.json(<path>)

    this will load in the parquet-serialized data as a DataFrame
     */
    val loadedJSONDF = sqlContext.read.json(path)

    /*
    to convert the DataFrame back to a standard SparkRDD just call
    DF.rdd (which transforms the DF into rdd[org.apache.spark.sql.Row])
     */
    val loadedSQLRowRDD: RDD[Row] = loadedJSONDF.rdd

    // then call a custom method to convert the sql.Row to your class, like them one below
    val loadedEventsRDD: RDD[Event] = loadedSQLRowRDD.map(EventHelper.fromSQLRDD)

    // now you're back in spark-core land, ready to do normal spark things
    loadedEventsRDD.map( e => (e.userName, 1) ).countByKey().foreach{ t =>
      println(t._1.toString + " : " + t._2.toString)
    }

  }

}



object EventsAsParquet {

  def main(args: Array[String]): Unit = {

    val startTime = (currentTimeMillis() / 1000.0).toInt

    val appName = "decision-stumps-one-test"
    val conf = new SparkConf().setAppName(appName).setMaster("local[16]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /*
    create some data in an RDD.  The data items are the Event class below
     */
    val eventsRDD: RDD[Event] = EventHelper.createRDDofFakeEvents(sc).cache()

    /*
    to convert the RDD[T] to a DataFrame you must either
      use the sqlContext.createDataFrame method
      or import all of the sqlContext implicit methods for RDDs
     */
    import sqlContext.implicits._
    // then convert the RDD[T] to a DataFrame,
    // if you've imported the sqlContext implicits, then use the .toDF method
    val eventsDF: DataFrame = eventsRDD.toDF()

    /*
    from here, writing proper parquet is as easy as calling
    DF.write.parquet(<path>)
    this will work with any filesystem that spark can write to.
    such as:
      Hadoop
      Amazon s3
      Tachyon
      Local filesystem
     */

    val path = "/tmp/testParquet"
    FileUtils.deleteQuietly(new File(path))
    eventsDF.write.parquet(path)

    /*
    loading in the parquet data is as easy as calling
    sqlContext.read.parquet(<path>)

    this will load in the parquet data as a DataFrame
     */
    val loadedParquetDF: DataFrame = sqlContext.read.parquet(path)

    /*
    to convert the DataFrame back to a standard SparkRDD just call
    DF.rdd (which transforms the DF into rdd[org.apache.spark.sql.Row])
     */
    val loadedSQLRowRDD: RDD[Row] = loadedParquetDF.rdd
    // then call a custom method to convert the sql.Row to your class, like them one below
    val loadedEventsRDD: RDD[Event] = loadedSQLRowRDD.map(EventHelper.fromSQLRDD)

    // now you're back in spark-core land, ready to do normal spark things
    loadedEventsRDD.map( e => (e.userName, 1) ).countByKey().foreach{ t =>
      println(t._1.toString + " : " + t._2.toString)
    }
  }

}


//easy DataFrame creation is achieved using case classes
case class Event(eventId: String, userName: String, timeStamp: Long) extends Serializable

/*
It's very convenient to pair your case class with a helper object
  with a fromSQLRDD method to convert loaded spark.sql.Row items into your case class is
 */
object EventHelper {

  val defaultNumEvents = 1000
  val defaultNumUsers = 25
  val defaultNumEventTypes = 25
  val defaultEndTs = 1443463667
  val defaultStartTs = 1388534400

  def getRandomTs(startTs: Int = defaultStartTs, entTs: Int = defaultEndTs): Long = Random.nextInt(entTs - startTs) + startTs
  def getRandomName(nUsers: Int = defaultNumUsers): String = "user_" + Random.nextInt(nUsers).toString
  def generateEventId(nEventTypes: Int = defaultNumEventTypes): String = "event_" + Random.nextInt(nEventTypes)

  def genRandomEvent: Event = Event(eventId = generateEventId(),userName = getRandomName(),timeStamp = getRandomTs())

  def createRDDofFakeEvents(sc: SparkContext, nEvents: Int = defaultNumEvents): RDD[Event] = {
    sc.parallelize(0 until nEvents).map{i => genRandomEvent}
  }

  def fromSQLRDD(row: Row): Event = {
    // to get the named fields from the Row, call row.getValuesMap with a Seq(<fieldsAsStrings>)
    //    of the fields you wish to retrieve
    val asMap = row.getValuesMap(Seq("eventId","userName","timeStamp"))
    Event(eventId = asMap("eventId"), userName = asMap("userName"), timeStamp = asMap("timeStamp"))
  }

}