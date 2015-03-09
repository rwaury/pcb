package de.tuberlin.dima.old.graph;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

@SuppressWarnings("serial")
public class AirportInfo implements Value {
		
	private double latitude = 0.0;
	private double longitude = 0.0;
		
	private int minConnectionTimeDomestic = 30;	
	private int minConnectionTimeInternational = 60;
	
	private int outgoingOffset = 0;
	
	// add terminal/minCT information
	
	private HashMap<OutgoingKey,Flight> seed = null; // all planes starting from this airport also for lookup of arrival times
	//private ArrayList<FlightConnection> outgoing = null; // all incoming connections eligible for a connecting flight / purged during each iteration
	private ArrayList<FlightConnection> arrived = null; // to dump all connections that end here
	
	public AirportInfo() {}
	
	public AirportInfo(double latitude, double longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}
	
	public AirportInfo(double latitude, double longitude, HashMap<OutgoingKey,Flight> seed, ArrayList<FlightConnection> arrived) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.seed = seed;
		if(arrived != null) {
			this.arrived = new ArrayList<FlightConnection>(arrived.size());
			for (FlightConnection conn : arrived) {
				this.arrived.add(new FlightConnection(conn));
			}
		} else {
			this.arrived = null;
		}
	}
	
	public void addSeedFlight(Flight f) {
		if(seed != null) {
			seed.put(new OutgoingKey(f.getDestination(), f.getDeparture()), f);
		} else {
			seed = new HashMap<OutgoingKey, Flight>();
			seed.put(new OutgoingKey(f.getDestination(), f.getDeparture()), f);
		}
	}
	
	public HashMap<OutgoingKey, Flight> getHashMap() {
		return this.seed;
	}
	
	public Collection<Flight> getSeed() {
		if(this.seed != null) {
			return this.seed.values();
		} else {
			return null;
		}
	}
	
	public int getOutgoingOffset() {
		return outgoingOffset;
	}
	
	public void setOutgoingOffset(int offset) {
		this.outgoingOffset = offset;
	}
	
	/*public void addOutgoingFlight(FlightConnection f) {
		if(outgoing != null) {
			outgoing.add(f);
		} else {
			outgoing = new ArrayList<FlightConnection>(1);
			outgoing.add(f);
		}
	}
	
	public ArrayList<FlightConnection> getOutgoingFlights() {
		return this.outgoing;
	}
	
	public void eraseOutgoingList() {
		if(this.outgoing != null) {
			this.outgoing.clear();
			this.outgoing.trimToSize();
			this.outgoing = null;
		}
	}*/
	
	public ArrayList<FlightConnection> getArrived() {
		return this.arrived;
	}
	
	public void addArrivedFlight(FlightConnection f) {
		if(arrived != null) {
			arrived.add(f);
		} else {
			arrived = new ArrayList<FlightConnection>(1);
			arrived.add(f);
		}
	}
	
	public int getArrivedSize() {
		if(arrived == null) {
			return 0;
		} else {
			return arrived.size();
		}
	}
	
	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	
	public int getMinConnectionTimeDomestic() {
		return minConnectionTimeDomestic;
	}

	public int getMinConnectionTimeInternational() {
		return minConnectionTimeInternational;
	}

	/*@Override
	public String toString() {
		if(seed != null) {
			StringBuilder sb = new StringBuilder(seed.size()*30);
			for(Flight f : seed.values()) {
				sb.append(f.toString() + "|");
			}
			return sb.toString();
		} else {
			return "";
		}
		
	}*/
	
	@Override
	public String toString() {
		if(arrived != null) {
			StringBuilder sb = new StringBuilder(arrived.size()*50);
			for(FlightConnection conn : arrived) {
				sb.append(conn.toString() + "|");
			}
			return sb.toString();
		} else {
			return "";
		}
		
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeDouble(latitude);
		out.writeDouble(longitude);
		out.writeInt(minConnectionTimeDomestic);
		out.writeInt(minConnectionTimeInternational);
		if(seed != null) {
			out.writeInt(seed.size());
			for(Flight f : seed.values()) {
				f.write(out);
			}
		} else {
			out.writeInt(0);
		}
		/*if(outgoing != null)	{
			out.writeInt(outgoing.size());
			for (FlightConnection conn : outgoing) {
				conn.write(out);
			}
		} else {
			out.writeInt(0);
		}*/
		if(arrived != null)	{
			out.writeInt(arrived.size());
			for (FlightConnection conn : arrived) {
				conn.write(out);
			}
		} else {
			out.writeInt(0);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.latitude = in.readDouble();
		this.longitude = in.readDouble();
		this.minConnectionTimeDomestic = in.readInt();
		this.minConnectionTimeInternational = in.readInt();
		int seedSize = in.readInt();
		if(seedSize > 0) {
			this.seed = new HashMap<OutgoingKey, Flight>(seedSize);
			for (int i = 0; i < seedSize; i++) {
				Flight f = new Flight();
				f.read(in);
				this.seed.put(new OutgoingKey(f.getDestination(), f.getDeparture()),f);
			}
		} else {
			this.seed = null;
		}
		/*int outgoingSize = in.readInt();
		if(outgoingSize > 0) {
			this.outgoing = new ArrayList<FlightConnection>(outgoingSize);
			for (int i = 0; i < outgoingSize; i++) {
				FlightConnection f = new FlightConnection();
				f.read(in);
				this.outgoing.add(f);
			}
		} else {
			this.outgoing = null;
		}*/
		int arrivedSize = in.readInt();
		if(arrivedSize > 0) {
			this.arrived = new ArrayList<FlightConnection>(arrivedSize);
			for (int i = 0; i < arrivedSize; i++) {
				FlightConnection f = new FlightConnection();
				f.read(in);
				this.arrived.add(f);
			}
		} else {
			this.arrived = null;
		}
	}
}