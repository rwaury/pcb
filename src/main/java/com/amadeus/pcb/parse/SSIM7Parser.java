package com.amadeus.pcb.parse;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class SSIM7Parser {
	
	public static class UTCExtractor implements 
	MapFunction<String, Tuple4<Long, Long, String, String>> {
		
		String[] tmp = null;
		
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"), Locale.FRENCH);
		
		SimpleDateFormat format = new SimpleDateFormat("yyMMddHHmm");
		
		@Override
		public Tuple4<Long, Long, String, String> map(String value) throws Exception {
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
				/*throw new Exception("Sanity check failed! Arrival equal to or earlier than departure.\n" +
						"Departure: " + departure.toString() + " Arrival: " + arrival.toString() + "\n"  + 
						"Sign: " + sign + " Hours: " + hours + " Minutes: " + minutes + "\n" +
						"Original value: " + value);*/
			}
			/* departure UTC timestamp, arrival UTC timestamp, origin airport, destination airport */
			return new Tuple4<Long, Long, String, String>
			(new Long(departure.getTime()), new Long(arrival.getTime()), tmp[1].trim(), tmp[2].trim());
		}
	}
	
	public static class FilteringUTCExtractor implements 
	FlatMapFunction<String, Tuple4<Long, Long, String, String>> {
		
		String[] tmp = null;
		
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"), Locale.FRENCH);
		
		SimpleDateFormat format = new SimpleDateFormat("yyMMddHHmm");
		
		Date earliest = new Date(1398902400000L); // 1405010000
		Date latest = new Date(1399420800000L); // 1405070000
		
		
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
			// only flights departing between 1405010000 and 1405070000
			if(departure.before(earliest) || departure.after(latest)) {
				return;
			}
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
				return;
				/*throw new Exception("Sanity check failed! Arrival equal to or earlier than departure.\n" +
						"Departure: " + departure.toString() + " Arrival: " + arrival.toString() + "\n"  + 
						"Sign: " + sign + " Hours: " + hours + " Minutes: " + minutes + "\n" +
						"Original value: " + value);*/
			}
			/* departure UTC timestamp, arrival UTC timestamp, origin airport, destination airport */
			out.collect(new Tuple4<Long, Long, String, String>
			(new Long(departure.getTime()), new Long(arrival.getTime()), tmp[1].trim(), tmp[2].trim()));
		}
	}
	
	public static class FictitiousPointFilter implements
	FilterFunction<Tuple4<Long, Long, String, String>> {

		ArrayList<String> fictitiousAirports = new ArrayList<String>() {{
			add("QFP");add("QMX");add("QMY");add("QPX");add("QPY");add("QTV");
			add("QUK");add("QXL");add("QXM");add("QXX");add("QXY");add("QZW");
			add("QZX");add("QZY");add("XCA");add("XXX");add("ZGZ");add("ZZG");
			}};
		
		@Override
		public boolean filter(Tuple4<Long, Long, String, String> value)
				throws Exception {
			if(fictitiousAirports.contains(value.f2) || fictitiousAirports.contains(value.f3)) {
				return false;
			} else {
				return true;
			}
		}

	}
	
	public static class MinMaxCountReducer implements
	GroupReduceFunction<Tuple4<Long, Long, String, String>, Tuple5<String, String, Integer, Long, Long>> {
		
		@Override
		public void reduce(Iterable<Tuple4<Long, Long, String, String>> values,
				Collector<Tuple5<String, String, Integer, Long, Long>> out)
				throws Exception {
			long max = 0L;
			long min = Long.MAX_VALUE;
			int count = 0;
			String origin = null;
			String destination = null;
			for (Tuple4<Long, Long, String, String> tuple4 : values) {
				if(count == 0) {
					origin = tuple4.f2;
					destination = tuple4.f3;
				}
				if(max < tuple4.f0.longValue()) {
					max = tuple4.f0.longValue();
				}
				if(min > tuple4.f0.longValue()) {
					min = tuple4.f0.longValue();
				}
				count++;
			}
			out.collect(new Tuple5<String, String, Integer, Long, Long>(origin, destination, count, min, max));
		}
	}
	
	public static class Join1 implements JoinFunction<Tuple4<Long, Long, String, String>, Tuple1<String>, Tuple4<Long, Long, String, String>> {

		@Override
		public Tuple4<Long, Long, String, String> join(
				Tuple4<Long, Long, String, String> first, Tuple1<String> second)
				throws Exception {
			return first;
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
		DataSet<String> flights = env.readTextFile("file:///home/robert/Amadeus/data/PRD.NGI.AIROS.SSIM7.D140206.txt");
		//DataSet<String> rawAirportInfo = env.readTextFile("file:///home/robert/Amadeus/data/ori_por_public.txt");
		CsvReader airportInput = env.readCsvFile("file:///home/robert/Amadeus/data/iata-airport-codes.txt").fieldDelimiter('\t').includeFields(true, false);
		
		DataSet<Tuple1<String>> airports = airportInput.types(String.class);
		
		//DataSet<Tuple4<Long, Long, String, String>> extracted = flights.map(new UTCExtractor());
		DataSet<Tuple4<Long, Long, String, String>> extracted = flights.flatMap(new FilteringUTCExtractor());
		//extracted.writeAsCsv("file:///home/robert/Amadeus/data/twoweeks", "\n", ",", WriteMode.OVERWRITE);
		
		// discard all connections that don't contain an IATA airport code on the list
		DataSet<Tuple4<Long, Long, String, String>> join1 = extracted.joinWithTiny(airports).where(2).equalTo(0).with(new Join1());
		DataSet<Tuple4<Long, Long, String, String>> join2 = join1.joinWithTiny(airports).where(3).equalTo(0).with(new Join1());
		
		// airport name, latitude, longitude
		//DataSet<Tuple3<String, Double, Double>> airportCoordinates = rawAirportInfo.flatMap(new AirportCoordinateExtractor());
		//airportCoordinates.writeAsCsv("file:///home/robert/Amadeus/data/coordinates", "\n", ",", WriteMode.OVERWRITE);
		
		DataSet<Tuple5<String, String, Integer, Long, Long>> aggregated = join2.groupBy(2,3).reduceGroup(new MinMaxCountReducer()).setParallelism(1);
		
		aggregated.writeAsCsv("file:///home/robert/Amadeus/data/result2", "\n", ",", WriteMode.OVERWRITE);
		
		//SpargelIteration iter = new SpargelIteration(arg0, arg1, arg2);
		//iter.setVertexInput(c);
				
		//filtered.writeAsCsv("file:///home/robert/Amadeus/data/result3/");
		
		env.execute("SSIM7 Filter");
	}

}
