package de.tuberlin.dima.old.parse;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class DistanceToTime {
	
	public static class SaneUTCExtractor implements 
	FlatMapFunction<String, Tuple4<Long, Long, String, String>> {
		
		String[] tmp = null;
		
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"), Locale.FRENCH);
		
		SimpleDateFormat format = new SimpleDateFormat("yyMMddHHmm");
		
		public void flatMap(String value, Collector<Tuple4<Long, Long, String, String>> out) throws Exception {
			tmp = value.split("\\^");
			String localDepartureDate = tmp[0].trim();
			String localDepartureTime = tmp[3].trim();
			String localDepartureOffset = tmp[4].trim();
			Date departure = format.parse(localDepartureDate + localDepartureTime);
			cal.setTime(departure);
			int sign = 0;
			if(localDepartureOffset.startsWith("-")) {
				sign = 1;
			} else if(localDepartureOffset.startsWith("+")) {
				sign = -1;
			} else {
				throw new Exception("Parse error. Wrong sign! Original String: " + value);
			}
			int hours = Integer.parseInt(localDepartureOffset.substring(1, 3));
			int minutes = Integer.parseInt(localDepartureOffset.substring(3, 5));
			cal.add(Calendar.HOUR_OF_DAY, sign*hours);
			cal.add(Calendar.MINUTE, sign*minutes);
			departure = cal.getTime();
			
			String localArrivalTime = tmp[5].trim();
			String localArrivalOffset = tmp[6].trim();
			String dateChange = tmp[7].trim();
			Date arrival = format.parse(localDepartureDate + localArrivalTime);
			cal.setTime(arrival);
			if(!dateChange.equals("0")) {
				if(dateChange.equals("1")) {
					cal.add(Calendar.DAY_OF_YEAR, 1);
				} else if(dateChange.equals("2")) {
					cal.add(Calendar.DAY_OF_YEAR, 2);
				} else if(dateChange.equals("A") || dateChange.equals("J")) {
					cal.add(Calendar.DAY_OF_YEAR, -1);
				} else {
					throw new Exception("Unknown arrival_date_variation modifier: " + dateChange + " original string: " + value);
				}
			}
			if(localArrivalOffset.startsWith("-")) {
				sign = 1;
			} else if(localArrivalOffset.startsWith("+")) {
				sign = -1;
			} else {
				throw new Exception("Parse error. Wrong sign!");
			}
			hours = Integer.parseInt(localArrivalOffset.substring(1, 3));
			minutes = Integer.parseInt(localArrivalOffset.substring(3, 5));
			cal.add(Calendar.HOUR_OF_DAY, sign*hours);
			cal.add(Calendar.MINUTE, sign*minutes);
			arrival = cal.getTime();
			
			// sanity check
			if(arrival.before(departure) || arrival.equals(departure)) {
				// ignore records with broken times
				return;
			}
			/* departure UTC timestamp, arrival UTC timestamp, origin airport, destination airport */
			out.collect(new Tuple4<Long, Long, String, String>
			(new Long(departure.getTime()), new Long(arrival.getTime()), tmp[1].trim(), tmp[2].trim()));
		}
	}
	
	public static class DistanceMapper implements MapFunction<Tuple8<Long, Long, String, Double, Double, String, Double, Double>, Tuple5<String, String, Long, Integer, Double>> {
		
		@Override
		public Tuple5<String, String, Long, Integer, Double> map(
				Tuple8<Long, Long, String, Double, Double, String, Double, Double> value)
				throws Exception {
			// TODO Compute duration of flight and distance between airports
			long duration = (value.f1.longValue() - value.f0.longValue())/1000; // in seconds
			int dist = dist(value.f3.doubleValue(), value.f4.doubleValue(), value.f6.doubleValue(), value.f7.doubleValue()); // in meters
			return new Tuple5<String, String, Long, Integer, Double>(value.f2, value.f5, duration, dist, (double)dist/(double)duration);
		}
		
		/*
		 * distance between two coordinates in meters
		 */
		private int dist(double lat1, double long1, double lat2, double long2) {
			double d2r = Math.PI / 180.0;			
			double dlong = (long2 - long1) * d2r;
		    double dlat = (lat2 - lat1) * d2r;
		    double a = Math.pow(Math.sin(dlat / 2.0), 2.0)
		            + Math.cos(lat1 * d2r)
		            * Math.cos(lat2 * d2r)
		            * Math.pow(Math.sin(dlong / 2.0), 2.0);
		    double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
		    double d = 6367000.0 * c;
		    return (int)d;
		}
	}
	
	public static class CoordinateJoiner1 implements JoinFunction<Tuple4<Long, Long, String, String>, Tuple3<String, Double, Double>, Tuple6<Long, Long, String, Double, Double, String>> {

		@Override
		public Tuple6<Long, Long, String, Double, Double, String> join(
				Tuple4<Long, Long, String, String> first, Tuple3<String, Double, Double> second)
				throws Exception {
			return new Tuple6<Long, Long, String, Double, Double, String>
			(first.f0, first.f1, first.f2, second.f1, second.f2, first.f3);
		}
		
	}
	
	public static class CoordinateJoiner2 implements JoinFunction<Tuple6<Long, Long, String, Double, Double, String>, Tuple3<String, Double, Double>, Tuple8<Long, Long, String, Double, Double, String, Double, Double>> {

		@Override
		public Tuple8<Long, Long, String, Double, Double, String, Double, Double> join(
				Tuple6<Long, Long, String, Double, Double, String> first, Tuple3<String, Double, Double> second)
				throws Exception {
			return new Tuple8<Long, Long, String, Double, Double, String, Double, Double>
			(first.f0, first.f1, first.f2, first.f3, first.f4, first.f5, second.f1, second.f2);
		}
		
	}

	public static void main(String[] args) throws Exception {
		/*
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
		DataSet<String> flights = env.readTextFile("file:///home/robert/Amadeus/data/PRD.NGI.AIROS.SSIM7.D140206.txt");
		DataSet<String> rawAirportInfo = env.readTextFile("file:///home/robert/Amadeus/data/ori_por_public.txt");
		CsvReader airportInput = env.readCsvFile("file:///home/robert/Amadeus/data/iata-airport-codes.txt").fieldDelimiter('\t').includeFields(true, false);
		DataSet<Tuple1<String>> airports = airportInput.types(String.class);
		
		DataSet<Tuple4<Long, Long, String, String>> extracted = flights.flatMap(new SaneUTCExtractor());

		// discard all connections that don't contain an IATA airport code on the list
		DataSet<Tuple4<Long, Long, String, String>> join1 = extracted.joinWithTiny(airports).where(2).equalTo(0).with(new Join1());
		DataSet<Tuple4<Long, Long, String, String>> join2 = join1.joinWithTiny(airports).where(3).equalTo(0).with(new Join1());
				
		// extract GPS coordinates of all known airports
		DataSet<Tuple3<String, Double, Double>> airportCoordinates = rawAirportInfo.flatMap(new AirportCoordinateExtractor());
		
		// add coordinates to connection data
		DataSet<Tuple6<Long, Long, String, Double, Double, String>> join3 = join2.joinWithTiny(airportCoordinates).where(2).equalTo(0).with(new CoordinateJoiner1());
		DataSet<Tuple8<Long, Long, String, Double, Double, String, Double, Double>> join4 = join3.joinWithTiny(airportCoordinates).where(5).equalTo(0).with(new CoordinateJoiner2());
		DataSet<Tuple5<String, String, Long, Integer, Double>> result = join4.map(new DistanceMapper()).groupBy(0,1).min(2);
		
		result.writeAsCsv("file:///home/robert/Amadeus/data/resultTimeMin", "\n", ",", WriteMode.OVERWRITE);
		join4.map(new DistanceMapper()).groupBy(0,1).max(2).writeAsCsv("file:///home/robert/Amadeus/data/resultTimeMax", "\n", ",", WriteMode.OVERWRITE);
		env.execute();
		*/
	}

}
