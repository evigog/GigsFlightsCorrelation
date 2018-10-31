import org.apache.spark._
import org.apache.spark.streaming._
import org.joda.time.DateTime
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat

object FlightGigs{
	def main(args: Array[String]) {

    // create Streaming context
    val conf = new SparkConf().setAppName("FlightGigsStream").setMaster("local[2]")
		val ssc = new StreamingContext(conf, Seconds(60))

    //checkpointing is necessary
    ssc.checkpoint("ckpt")

    // create Songkick Stream from csv 
    // /Users/evi/Documents/KTH_ML/Period_5/DataIntensiveComputing/Project/
    val gigs_stream = ssc.textFileStream("file:///mnt/c/Users/horst/Documents/KTH/3RD/ID2221/project/GigsFlightsCorrelation/spark_component/data/songkick_data") 
    
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
    //gigs_data.print()

    //gigs_data.saveAsTextFiles("output")

    // create Flight Stream from csv
    val flight_stream = ssc.textFileStream("file:///mnt/c/Users/horst/Documents/KTH/3RD/ID2221/project/GigsFlightsCorrelation/spark_component/data/flight_data") 

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
    //flight_data.print()

    // STEP1: window the streams
    //val gigs_window = gigs_data.window(Minutes(5))  // window size multiple of batch interval!
    //val flight_window = flight_data.window(Seconds(10)) // window size multiple of batch interval!

    // STEP2: join the two streams on the event_city, end
    val gig_flight_joined = gigs_data.join(flight_data) //result: (event_city, event_obj, flight_obj)
    //gig_flight_joined.print()

    // STEP3: filter data on flight period (concert_date-2, concert_date)
    val gig_flight_flight_period = (gig_flight_joined.filter(tuple => 
    tuple._2._2.flight_date.isAfter(tuple._2._1.event_date.minusDays(180)) &&
    tuple._2._2.flight_date.isBefore(tuple._2._1.event_date)))
    //gig_flight_flight_period.print()

    // STEP4: change key to (event_city,event_date)
    val gig_flight =  gig_flight_flight_period.map(tuple => ((tuple._1, tuple._2._1.event_date), (tuple._2._1, tuple._2._2)))
    //gig_flight.print()

    // STEP5: compute avg price per different key and updateState
    def update_state (key: (String,DateTime), value: Option[(Event, Flight_Info)], state: State[(Double, Integer)]) :Option[((String,DateTime), Double)] = {
      val v = value.get
      val newPrice = v._2.price
      if (state.exists()) {
        val oldState = state.get()
        val sum = newPrice + oldState._1
        val count = oldState._2 + 1

      state.update((sum,count))
      Some(key, (sum/count).toDouble)
      }
      else {
        val sum = newPrice
        val count = 1

      state.update((sum,count))
      Some(key, (sum/count).toDouble)
      }
    }

    val avg_price_stream = gig_flight.mapWithState(StateSpec.function(update_state _))
    avg_price_stream.print()
//
//    print("Hereeeeeeeeeeeeeee ",avg_price_stream.print())
//
//    // save result 
//    avg_price_stream.saveAsTextFiles("output")

    ssc.start()
    ssc.awaitTermination()

	}

}