package de.tuberlin.dima.old.spargel;

import de.tuberlin.dima.old.graph.*;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.OutgoingEdge;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public class ConnectionMessenger extends MessagingFunction<String, AirportInfo, FlightConnection, ConnectionInfo> {
	
	private static final int MAX_CONNECTIONS = 1;
	
	private static final double GEO_DETOUR_3LEG_THRESHOLD = 1.5;
	
	private AirportInfo vertex = null;
	
	private String vertexName = null;
	
	private HashMap<String, ConnectionInfo> targetMap = null;
	
	private HashMap<String, Coordinate> geoInfo = null;
	
	private long minCTDomestic = 0;
	private long minCTInternational = 0;
	
	@Override
	public void sendMessages(String vertexKey, AirportInfo vertexValue)
			throws Exception {
		if(this.getSuperstepNumber() == 1) {
			// send the whole seed during the first step
			if(vertexValue.getSeed() != null) {
				for (Flight f : vertexValue.getSeed()) {
					sendMessageTo(f.getDestination(), new FlightConnection(f));
				}
			}
		} else {
			this.vertex = vertexValue;
			this.vertexName = vertexKey;
			buildTargetMap();
			if(this.targetMap.size() == 0) {
				//vertexValue.eraseOutgoingList();
				return;
			}
			buildGeoInfo();
			/*ArrayList<FlightConnection> outgoing = vertexValue.getOutgoingFlights();
			if(outgoing == null || outgoing.size() == 0) {
				vertexValue.eraseOutgoingList();
				return;
			}*/
			// TODO: lookup on minCT data
			this.minCTDomestic = vertexValue.getMinConnectionTimeDomestic()*60L*1000L;
			this.minCTInternational = vertexValue.getMinConnectionTimeInternational()*60L*1000L;
			Flight[] flights = null;
			boolean isDomestic = false;
			String target = null;
			FlightConnection conn = null;
			for(int i = vertex.getOutgoingOffset(); i < vertex.getArrivedSize(); i++) {
				conn = vertex.getArrived().get(i);
				if(!conn.getLastFlight().getDestination().equals(vertexKey))
					throw new Exception("Wrong flight in outgoing list! Expected: " + vertexKey + " Got: " + conn.toString());
				for(Entry<String, ConnectionInfo> edge : this.targetMap.entrySet()) {
					target = edge.getKey();
					if(geoDetourLimitObserved(conn, edge.getValue().getDestinationLatitude(), 
							edge.getValue().getDestinationLongitude())) {
						isDomestic = isDomestic(vertexKey, target);
						flights = getFlights(target, conn, isDomestic);
						if(flights != null) {
							for (int j = 0; j < flights.length; j++) {
								FlightConnection c = new FlightConnection(conn);
								c.addFlight(flights[j]);
								sendMessageTo(target, c);
							}
						}
					}
				}
			}
			/*Iterator<FlightConnection> outgoingFlights = outgoing.iterator();
			while(outgoingFlights.hasNext()) {
				conn = outgoingFlights.next();
				if(!conn.getLastFlight().getDestination().equals(vertexKey))
					throw new Exception("Wrong flight in outgoing list! Expected: " + vertexKey + " Got: " + conn.toString());
				for(Entry<String, ConnectionInfo> edge : this.targetMap.entrySet()) {
					target = edge.getKey();
					if(geoDetourLimitObserved(conn, edge.getValue().getDestinationLatitude(), 
							edge.getValue().getDestinationLongitude())) {
						isDomestic = isDomestic(vertexKey, target);
						flights = getFlights(target, conn, isDomestic);
						if(flights != null) {
							for (int i = 0; i < flights.length; i++) {
								FlightConnection c = new FlightConnection(conn);
								c.addFlight(flights[i]);
								sendMessageTo(target, c);
							}
						}
					}
				}
				outgoingFlights.remove();
			}
			outgoing.trimToSize();*/
		}
	}

	private Flight[] getFlights(String target, FlightConnection conn, boolean isDomestic) throws Exception {
		long arrival = conn.getLastFlight().getArrival();
		ConnectionInfo connInfo = targetMap.get(target);
		long earliest = 0L;
		long latest = 0L;
		//TODO: minCT lookup
		if(isDomestic) {
			earliest = arrival + minCTDomestic;
		} else {
			earliest = arrival + minCTInternational;
		}
		long maxCT = 0L;
		Coordinate originCoordinate = this.geoInfo.get(conn.getFirstFlight().getOrigin());
		Coordinate destinationCoordinate = this.geoInfo.get(target);
		double ODDist = dist(originCoordinate.getLatitude(), originCoordinate.getLongitude(), 
				destinationCoordinate.getLatitude(), destinationCoordinate.getLongitude());
		maxCT = (long) (180.0*Math.log(ODDist+1000.0)-1000.0);
		maxCT *= 60L*1000L;
		// TODO: handle domestic DEP-ARR via international hub case
		// FIXME: won't work for leg size larger than 3
		if(conn.getSize() == 2)
			maxCT -= (conn.getLastFlight().getArrival()-conn.getFirstFlight().getDeparture());
		latest = earliest + maxCT;
		long[] timestamps = connInfo.getBitmap().getFirstNHits(earliest, latest, MAX_CONNECTIONS);
		if(timestamps == null)
			return null;
		Flight[] result = new Flight[timestamps.length];
		for (int i = 0; i < timestamps.length; i++) {
			result[i] = vertex.getHashMap().get(new OutgoingKey(target, timestamps[i]));
			if(result[i] == null)
				throw new Exception("invalid entry: " + this.vertexName + " " + target + " index: " + i);
		}
		return result;
	}

	private boolean isDomestic(String origin, String destination) {
		Coordinate originCoordinate = this.geoInfo.get(origin);
		Coordinate destinationCoordinate = this.geoInfo.get(destination);
		return originCoordinate.getCountryCode().equals(destinationCoordinate.getCountryCode());
	}
	
	/*
	 * distance between two coordinates in kilometers
	 */
	private double dist(double lat1, double long1, double lat2, double long2) {
		double d2r = Math.PI / 180.0;			
		double dlong = (long2 - long1) * d2r;
	    double dlat = (lat2 - lat1) * d2r;
	    double a = Math.pow(Math.sin(dlat / 2.0), 2.0)
	            + Math.cos(lat1 * d2r)
	            * Math.cos(lat2 * d2r)
	            * Math.pow(Math.sin(dlong / 2.0), 2.0);
	    double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
	    double d = 6367.0 * c;
	    return d;
	}
	
	private boolean geoDetourLimitObserved(FlightConnection conn, double destLatitude, double destLongitude) throws Exception {
		double legDistanceSum = 0;
		String firstAirport = conn.getFirstFlight().getOrigin();
		Coordinate origin = null;
		Coordinate destination = null;
		for(Flight f : conn.getFlights()) {
			origin = this.geoInfo.get(f.getOrigin());
			destination = this.geoInfo.get(f.getDestination());
			if(origin == null || destination == null) {
				throw new Exception("Error retrieving geo info!");
			}
			legDistanceSum += dist(origin.getLatitude(), origin.getLongitude(), 
					destination.getLatitude(), destination.getLongitude());
		}
		legDistanceSum += dist(this.vertex.getLatitude(), this.vertex.getLongitude(), destLatitude, destLongitude);
		origin = this.geoInfo.get(firstAirport);
		double ODdistance = dist(origin.getLatitude(), origin.getLongitude(), destLatitude, destLongitude);
		double geoDetour = legDistanceSum/ODdistance;
		double maxDetour = Math.min(2.5,-0.365*Math.log(ODdistance)+4.8);
		if(conn.getSize() >= 2)
			maxDetour = GEO_DETOUR_3LEG_THRESHOLD;
		return (geoDetour <= maxDetour);
	}
	
	private void buildTargetMap() throws CloneNotSupportedException {
		this.targetMap = new HashMap<String, ConnectionInfo>();
		Iterable<OutgoingEdge<String, ConnectionInfo>> edges = getOutgoingEdges();
		for (OutgoingEdge<String, ConnectionInfo> edge : edges) {
			this.targetMap.put(edge.target(), edge.edgeValue().clone());
		}
	}
	
	private void buildGeoInfo() {
		if(this.geoInfo == null) {
			this.geoInfo = new HashMap<String, Coordinate>();
			Collection<Tuple4<String, String, Double, Double>> broadcastSet = 
					this.getBroadcastSet(AirportGraphComputation.GEO_DATA_SET_NAME);
			for (Iterator<Tuple4<String, String, Double, Double>> iterator = broadcastSet.iterator(); iterator.hasNext();) {
				Tuple4<String, String, Double, Double> tuple4 = (Tuple4<String, String, Double, Double>) iterator.next();
				this.geoInfo.put(tuple4.f0, new Coordinate(tuple4.f2, tuple4.f3, tuple4.f1));
			}
		}
	}
	
}