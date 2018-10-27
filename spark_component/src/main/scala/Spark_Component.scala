import org.apache.spark._
import org.apache.spark.streaming._

object FlightGigs{
	def main(args: Array[String]) {

		// create Streaming context
		val ssc = new StreamingContext("local[2]", "FlightGigsStream", Seconds(10),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass).toSeq)

    //checkpointing is necessary
    ssc.checkpoint("ckpt")

    	// create Songkick Stream from csv 
      // /Users/evi/Documents/KTH_ML/Period_5/DataIntensiveComputing/Project/
    	val gigs_stream = ssc.textFileStream("data/songkick_data") 
      
      case class Event(
      concert_id: Integer,
      artist: String,
      event_date: Long,
      event_city: String
      )

      val gigs_data = gigs_stream map { line =>
      val Array(concert_id, artist, event_date, event_city) = line.split('\t')

      val event_obj = Event(concert_id.toInt, artist, event_date.toLong, event_city)
      event_city -> event_obj
      }

      //gigs_data.saveAsTextFiles("output")

      // create Flight Stream from csv
      val flight_stream = ssc.textFileStream("data/flight_data") 

      case class Flight_Info(
      start: String,
      end: String,
      flight_date: Long,
      price: Double
      )

      val flight_data = flight_stream map { line =>
      val Array(start, end, flight_date, price) = line.split('\t')

      val flight_obj = Flight_Info(start, end, flight_date.toLong, price.toDouble)
      end -> flight_obj
      }
      //flight_data.print()

    // STEP1: window the streams
   // val gigs_window = gigs_data.window(Minutes(5))  // window size multiple of batch interval!
    //val flight_window = flight_data.window(Seconds(10)) // window size multiple of batch interval!


    // STEP2: join the two streams on the event_city, end
    val gig_flight_joined = gigs_data.join(flight_data) //result: (event_city, event_obj, flight_obj)

    // STEP3: filter data on flight period (concert_date-2, concert_date)
    val gig_flight_flight_period = gig_flight_joined.filter(tuple => tuple._2._2.flight_date >= tuple._2._1.event_date-2 & tuple._2._2.flight_date < tuple._2._1.event_date)

    gig_flight_flight_period.print()

    // STEP4: change key to (event_city,event_date)
    val gig_flight =  gig_flight_flight_period.map(tuple => ( (tuple._1, tuple._2._1.event_date), (tuple._2._1, tuple._2._2)) )  

    // STEP5: compute avg price per different key and updateState
    val update_state = (key: (String,Long), value: Option[(Event, Flight_Info)], state: State[(Double, Integer)]) => {
        val newPrice_any = value.getOrElse(0) //Any type
        val newPrice = { newPrice_any match {
              case tuple @ (a: Event, b: Flight_Info) => b.price
          }
        }
        val oldState_any = state.getOption.getOrElse(0) //Any type
        val oldState =  { oldState_any match {
              case tuple @ (a: Double, b: Integer) => (a, b)
            }
        }
        val sum = newPrice + oldState._1
        val count = oldState._2 + 1
        state.update((sum, count))
        (key, sum/count)
      }

    val avg_price_stream = gig_flight.mapWithState(StateSpec.function(update_state))

    // save result 
   avg_price_stream.saveAsTextFiles("output")





    ssc.start()
    ssc.awaitTermination()



	}

}