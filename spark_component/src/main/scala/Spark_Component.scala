//import org.apache.spark._
import org.apache.spark.streaming._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

//import org.apache.spark.sql.SQLContext
//import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.streaming._    

import org.apache.spark.storage.StorageLevel
//import org.apache.spark.Logging
//import org.apache.log4j.{Level, Logger}

import org.joda.time.DateTime
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat

object FlightGigs{
    def main(args: Array[String]) {

    // create Streaming context
    val conf = new SparkConf().setAppName("FlightGigsStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(22))

    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "127.0.0.1:9200") //elastic server ip
    conf.set("es.nodes.wan.only", "false")

    //checkpointing is necessary
    ssc.checkpoint("ckpt")

    val elasticResource = "spark_results/docs"

    // create Songkick Stream from csv 
    val gigs_stream = ssc.textFileStream("file:///Users/evigogoulou/Documents/KTH_ML/Period_5/DataIntensiveComputing/Project/GigsFlightsCorrelation/spark_component/data/songkick_data") 
    
    case class Event(
    concert_id: Integer,
    artist: String,
    event_date: DateTime,
    event_city: String
    )

        val gigs_data = gigs_stream map { line =>
        val Array(concert_id, artist, event_date, event_city ) = line.split(",")
        val event_obj = Event(concert_id.toInt, artist, DateTime.parse(event_date), event_city)
        event_city -> event_obj
        }
    gigs_data.print()

    // create Flight Stream from csv
    val flight_stream = ssc.textFileStream("file:///Users/evigogoulou/Documents/KTH_ML/Period_5/DataIntensiveComputing/Project/GigsFlightsCorrelation/spark_component/data/flight_data") 

    case class Flight_Info(
    start: String,
    end: String,
    flight_date: DateTime,
    price: Double
    )

    val flight_data = flight_stream map { line =>
    val Array(start, end, flight_date, price) = line.split(",")
    val flight_obj = Flight_Info(start, end, DateTime.parse(flight_date), price.toDouble)
    end -> flight_obj
    }

    // join streams on city destination
    val gig_flight_joined = gigs_data.join(flight_data) //result: (event_city, event_obj, flight_obj)

    // filter out records with flight date before event_date - 5
    val gig_flight_period = (gig_flight_joined.filter(tuple => 
    tuple._2._2.flight_date.isAfter(tuple._2._1.event_date.minusDays(5)) &&
    tuple._2._2.flight_date.isBefore(tuple._2._1.event_date)))
   

    // change key to (city, event_date)
    val gig_flight =  gig_flight_flight_period.map(tuple => ((tuple._1, tuple._2._1.event_date), (tuple._2._1, tuple._2._2)))
    

    // compute avg price per different key and updateState
    def update_state (key: (String,DateTime), value: Option[(Event, Flight_Info)], state: State[(Double, Integer)]) :((String,DateTime), Double)= {
      val v = value.get
      val newPrice = v._2.price
      if (state.exists()) {
        val oldState = state.get()
        val sum = newPrice + oldState._1
        val count = oldState._2 + 1

      state.update((sum,count))
      
      (key, (sum/count).toDouble)
      }
      else {
        val sum = newPrice
        val count = 1

      state.update((sum,count))
      
      (key, (sum/count).toDouble)
      }
    }

    val avg_price_stream = gig_flight.mapWithState(StateSpec.function(update_state _))
    avg_price_stream.print()

    //convert data to Map and send it to Elasticsearch
    val avg_price_string = avg_price_stream.map(res => Map("event_city" -> res._1._1.toString, "event_date" -> res._1._2.toString, "avg_price"->res._2))

    avg_price_string.saveToEs(elasticResource)

    ssc.start()
    ssc.awaitTermination()

    }

}