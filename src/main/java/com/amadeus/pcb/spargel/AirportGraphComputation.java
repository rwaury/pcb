package com.amadeus.pcb.spargel;

import com.amadeus.pcb.graph.AirportInfo;
import com.amadeus.pcb.graph.ConnectionInfo;
import com.amadeus.pcb.graph.Flight;
import com.amadeus.pcb.graph.FlightConnection;
import com.amadeus.pcb.util.SerializationUtils;
import com.amadeus.pcb.util.TimeResolution;
import com.amadeus.pcb.util.VanillaBitmap;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class AirportGraphComputation {
	
	public static final String GEO_DATA_SET_NAME = "AirportGeoData";
	
	public static final int MAX_LEGS = 2;
	public static final int MAX_ITERATIONS = MAX_LEGS + 1;
	
	public static final long START 	= 1399002400000L;//1398902400000L;
	public static final long END 	= 1399107140000L;//1399507140000L;

	private static final String BROADCASTSET_PATH = "file:///home/robert/Amadeus/data/AirportCoordinates/";
	private static final String VERTEX_PATH = "file:///home/robert/Amadeus/data/Vertices/";
	private static final String EDGE_PATH = "file:///home/robert/Amadeus/data/Edges/";
	
	@SuppressWarnings("serial")
	public static class FilteringUTCExtractor implements 
	FlatMapFunction<String, Tuple7<String, String, String, String, String, Long, Long>> {
		
		private String[] tmp = null;
		
		private Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"), Locale.FRENCH);
		
		private SimpleDateFormat format = new SimpleDateFormat("yyMMddHHmm");
		
		private Date earliest = new Date(START); // 1405040000
		private Date latest = new Date(END); // 1405070000
		
		private final static ArrayList<String> nonAirCraftList = new ArrayList<String>() {{
			add("AGH");add("BH2");add("BUS");add("ICE");
			add("LCH");add("LMO");add("MD9");add("NDE");
			add("S61");add("S76");add("TRN");add("TSL");
			}};
		
		
		public void flatMap(String value, Collector<Tuple7<String, String, String, String, String, Long, Long>> out) throws Exception {
			tmp = value.split("\\^");
			if(nonAirCraftList.contains(tmp[9].trim()))
				return;
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
			String originTerminal = null;
			if(tmp[19].trim().length() == 2) {
				originTerminal = tmp[19].trim();
			} else if(tmp[19].trim().length() == 1) {
				originTerminal = SerializationUtils.PADDING_CHAR + tmp[19].trim();
			} else {
				originTerminal = new String(SerializationUtils.PADDING_CHAR + SerializationUtils.PADDING_CHAR);
			}
			String destinationTerminal = null;
			if(tmp[20].trim().length() == 2) {
				destinationTerminal = tmp[19].trim();
			} else if(tmp[20].trim().length() == 1) {
				destinationTerminal = SerializationUtils.PADDING_CHAR + tmp[19].trim();
			} else {
				// no terminal information provided
				destinationTerminal = new String(SerializationUtils.PADDING_CHAR + SerializationUtils.PADDING_CHAR);
			}
			/* airline, origin airport, origin terminal, destination airport, destination terminal, departure UTC timestam, arrival UTC timestamp */
			out.collect(new Tuple7<String, String, String, String, String, Long, Long>
			(tmp[8].trim(), tmp[1].trim(), originTerminal, tmp[2].trim(), destinationTerminal, new Long(departure.getTime()), new Long(arrival.getTime())));
		}
	}
	
	@SuppressWarnings("serial")
	public static class AirportCoordinateExtractor implements 
	FlatMapFunction<String, Tuple4<String, String, Double, Double>> {
		
		private String[] tmp = null;
		private String from = null;
		private String until = null;
		
		private final static ArrayList<String> fictitiousAirports = new ArrayList<String>() {{
			add("QFP");add("QMX");add("QMY");add("QPX");add("QPY");add("QTV");
			add("QUK");add("QXL");add("QXM");add("QXX");add("QXY");add("QZW");
			add("QZX");add("QZY");add("XCA");add("XXX");add("ZGZ");add("ZZG");
			}};
		
		@Override
		public void flatMap(String value, Collector<Tuple4<String, String, Double, Double>> out) throws Exception {
			tmp = value.split("\\^");
			if(fictitiousAirports.contains(tmp[0].trim())) {
				// not a real airport
				return;
			}
			if(!tmp[41].trim().equals("A")) {
				// not an airport
				return;
			}
			from = tmp[13].trim();
			until = tmp[14].trim();
			if(!until.equals("")) {
				// expired
				return;
			}
			Double latitude;
			Double longitude;
			try {
				latitude = new Double(Double.parseDouble(tmp[8].trim()));
				longitude = new Double(Double.parseDouble(tmp[9].trim()));
			} catch(Exception e) {
				// invalid coordinates
				return;
			}
			out.collect(new Tuple4<String, String, Double, Double>(tmp[0].trim(), tmp[16].trim(), latitude, longitude));
		}
	}
	
	@SuppressWarnings("serial")
	public static class Join1 implements JoinFunction<Tuple7<String, String, String, String, String, Long, Long>, Tuple4<String, String, Double, Double>, Tuple7<String, String, String, String, String, Long, Long>> {

		@Override
		public Tuple7<String, String, String, String, String, Long, Long> join(
				Tuple7<String, String, String, String, String, Long, Long> first, Tuple4<String, String, Double, Double> second)
				throws Exception {
			return first;
		}
		
	}
	
	@SuppressWarnings("serial")
	public static class JoinE implements JoinFunction<Tuple3<String, String, ConnectionInfo>, Tuple4<String, String, Double, Double>, Tuple3<String, String, ConnectionInfo>> {

		@Override
		public Tuple3<String, String, ConnectionInfo> join(
				Tuple3<String, String, ConnectionInfo> first, Tuple4<String, String, Double, Double> second)
				throws Exception {
			first.f2.setDestinationLatitude(second.f2.doubleValue());
			first.f2.setDestinationLongitude(second.f3.doubleValue());
			return first;
		}
		
	}
	
	@SuppressWarnings("serial")
	public static class EdgeCreator implements GroupReduceFunction<Tuple7<String, String, String, String, String, Long, Long>, Tuple3<String, String, ConnectionInfo>> {
		
		@Override
		public void reduce(Iterable<Tuple7<String, String, String, String, String, Long, Long>> values,
				Collector<Tuple3<String, String, ConnectionInfo>> out)
				throws Exception {
			ConnectionInfo conn = new ConnectionInfo();
			VanillaBitmap bitmap = new VanillaBitmap(START, END, TimeResolution.MINUTE);
			Tuple7<String, String, String, String, String, Long, Long> tuple7 = new Tuple7<String, String, String, String, String, Long, Long>();
			Iterator<Tuple7<String, String, String, String, String, Long, Long>> iterator = values.iterator();
			while(iterator.hasNext()) {
				tuple7 = iterator.next();
				bitmap.addFlight(tuple7.f5.longValue());
			}
			conn.setBitmap(bitmap);
			out.collect(new Tuple3<String, String, ConnectionInfo>(tuple7.f1, tuple7.f3, conn));
		}
		
	}
	
	@SuppressWarnings("serial")
	public static class AirportSeeder implements CoGroupFunction<Tuple2<String, AirportInfo>, Tuple7<String, String, String, String, String, Long, Long>, Tuple2<String, AirportInfo>> {

		@Override
		public void coGroup(Iterable<Tuple2<String, AirportInfo>> first,
				Iterable<Tuple7<String, String, String, String, String, Long, Long>> second,
				Collector<Tuple2<String, AirportInfo>> out) throws Exception {
			Iterator<Tuple2<String, AirportInfo>> iter = first.iterator();
			if(iter.hasNext()) {
				Tuple2<String, AirportInfo> result = iter.next();
				for (Tuple7<String, String, String, String, String, Long, Long> tuple7 : second) {
					result.f1.addSeedFlight(new Flight(tuple7.f0, tuple7.f1, tuple7.f2, tuple7.f3, tuple7.f4, tuple7.f5, tuple7.f6));
				}
				out.collect(result);
			}
			if(iter.hasNext()) {
				throw new Exception("Only one tuple expected: " + iter.next().f0);
			}
		}
		
	}
	
	@SuppressWarnings("serial")
	public static class EmptyAirportMapper implements MapFunction<Tuple4<String, String, Double, Double>, Tuple2<String, AirportInfo>> {

		@Override
		public Tuple2<String, AirportInfo> map(
				Tuple4<String, String, Double, Double> value)
				throws Exception {
			return new Tuple2<String, AirportInfo>(value.f0, new AirportInfo(value.f2, value.f3));
		}
		
	}
	
	private static void buildBroadCastSet() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
		DataSet<String> rawAirportInfo = env.readTextFile("file:///home/robert/Amadeus/data/ori_por_public.txt");
		// extract GPS coordinates of all known airports
		DataSet<Tuple4<String, String, Double, Double>> airportCoordinates = rawAirportInfo.flatMap(new AirportCoordinateExtractor());
		airportCoordinates.writeAsCsv(BROADCASTSET_PATH, "\n", ",", WriteMode.OVERWRITE);
		
		env.execute("Airport Coordinate Job");
	}
	
	private static void buildGraph() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
		DataSet<Tuple4<String, String, Double, Double>> airportCoordinates = env.readCsvFile(BROADCASTSET_PATH).types(String.class, String.class, Double.class, Double.class);
		
		// get all relevant schedule data
		DataSet<String> flights = env.readTextFile("file:///home/robert/Amadeus/data/oneweek.txt");
		DataSet<Tuple7<String, String, String, String, String, Long, Long>> extracted = flights.flatMap(new FilteringUTCExtractor());

		// discard all connections that don't contain an IATA airport code
		DataSet<Tuple7<String, String, String, String, String, Long, Long>> join1 = extracted.joinWithTiny(airportCoordinates).where(1).equalTo(0).with(new Join1());
		DataSet<Tuple7<String, String, String, String, String, Long, Long>> join2 = join1.joinWithTiny(airportCoordinates).where(3).equalTo(0).with(new Join1());
			
		// create airport vertices
		DataSet<Tuple2<String, AirportInfo>> vertices = airportCoordinates.map(new EmptyAirportMapper());
		DataSet<Tuple2<String, AirportInfo>> verticesWithSeed = vertices.coGroup(join2).where(0).equalTo(1).with(new AirportSeeder());
		
		// create flight connection edges
		DataSet<Tuple3<String, String, ConnectionInfo>> edges = join2.groupBy(1,3).reduceGroup(new EdgeCreator());
		DataSet<Tuple3<String, String, ConnectionInfo>> edgesWithCoordinates = edges.joinWithTiny(airportCoordinates).where(1).equalTo(0).with(new JoinE());
		
		verticesWithSeed.write(new VertexOutputFormat(), VERTEX_PATH, WriteMode.OVERWRITE);
		edgesWithCoordinates.write(new EdgeOutputFormat(), EDGE_PATH, WriteMode.OVERWRITE);
		//verticesWithSeed.writeAsText(VERTEX_PATH, WriteMode.OVERWRITE);
		//edgesWithCoordinates.writeAsText(EDGE_PATH, WriteMode.OVERWRITE);

		env.execute("Graph Builder Job");
				
	}

	public static void main(String[] args) throws Exception {
		boolean runIteration = true;
		//buildBroadCastSet();
		//buildGraph();
		if(runIteration) {
			ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
			// load broadcast set
			DataSet<Tuple4<String, String, Double, Double>> airportCoordinates = env.readCsvFile(BROADCASTSET_PATH).types(String.class, String.class, Double.class, Double.class);

			// load graph
			DataSet<Tuple2<String, AirportInfo>> vertices = env.readFile(new VertexInputFormat(), VERTEX_PATH);
			DataSet<Tuple3<String, String, ConnectionInfo>> edges = env.readFile(new EdgeInputFormat(), EDGE_PATH);
		
			// run iteration
			VertexCentricIteration<String, AirportInfo, FlightConnection, ConnectionInfo> iteration = VertexCentricIteration.withValuedEdges(edges, new AirportVertex(), new ConnectionMessenger(), MAX_ITERATIONS);
			iteration.addBroadcastSetForMessagingFunction(GEO_DATA_SET_NAME, airportCoordinates);
			DataSet<Tuple2<String, AirportInfo>> result = vertices.runOperation(iteration);
		
			result.writeAsText("file:///home/robert/Amadeus/data/SpargelOutput/", WriteMode.OVERWRITE);
			env.execute("AirportGraph Job");
		}
	}

}
