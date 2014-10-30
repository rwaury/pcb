package com.amadeus.pcb.graph;

import com.amadeus.pcb.util.SerializationUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

@SuppressWarnings("serial")
public class Flight implements Value, Cloneable {
			
	private String airline = null; // always 2 characters
	private String origin = null; // always 3 characters
	private String originTerminal = null; // at most 2 characters
	private String destination = null; // always 3 characters
	private String destinationTerminal = null; // at most 2 characters
	private long departure = 0L;
	private long arrival = 0L;
	
	public Flight() {}
	
	public Flight(String airline, String origin, String destination, long departure, long arrival) {
		this.airline = airline;
		this.origin = origin;
		this.originTerminal = new String(SerializationUtils.PADDING_CHAR + SerializationUtils.PADDING_CHAR);
		this.destination = destination;
		this.destinationTerminal = new String(SerializationUtils.PADDING_CHAR + SerializationUtils.PADDING_CHAR);
		this.departure = departure;
		this.arrival = arrival;
	}
	
	public Flight(String airline, String origin, String originTerminal, 
			String destination, String destinationTerminal, long departure, long arrival) {
		this.airline = airline;
		this.origin = origin;
		this.originTerminal = originTerminal;
		this.destination = destination;
		this.destinationTerminal = destinationTerminal;
		this.departure = departure;
		this.arrival = arrival;
	}

	public String getAirline() {
		return airline;
	}

	public String getOrigin() {
		return origin;
	}
	
	public String getOriginTerminal() {
		return originTerminal;
	}

	public String getDestination() {
		return destination;
	}
	
	public String getDestinationTerminal() {
		return destinationTerminal;
	}

	public long getDeparture() {
		return departure;
	}

	public long getArrival() {
		return arrival;
	}
	
	@Override
	public void write(DataOutputView out) throws IOException {
		/*
		SerializationUtils.serializeFixedLengthString(out, airline);
		SerializationUtils.serializeFixedLengthString(out, origin);
		SerializationUtils.serializeFixedLengthString(out, originTerminal);
		SerializationUtils.serializeFixedLengthString(out, destination);
		SerializationUtils.serializeFixedLengthString(out, destinationTerminal);
		*/
		out.writeUTF(airline);
		out.writeUTF(origin);
		out.writeUTF(originTerminal);
		out.writeUTF(destination);
		out.writeUTF(destinationTerminal);
		out.writeLong(departure);
		out.writeLong(arrival);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		/*
		this.airline = SerializationUtils.deserializeFixedLengthString(in, SerializationUtils.IATA_AIRLINE_CODE_LEN);
		this.origin = SerializationUtils.deserializeFixedLengthString(in, SerializationUtils.IATA_AIRPORT_CODE_LEN);
		this.originTerminal = SerializationUtils.deserializeFixedLengthString(in, SerializationUtils.TERMINAL_CODE_LEN);
		this.destination = SerializationUtils.deserializeFixedLengthString(in, SerializationUtils.IATA_AIRPORT_CODE_LEN);
		this.destinationTerminal = SerializationUtils.deserializeFixedLengthString(in, SerializationUtils.TERMINAL_CODE_LEN);
		*/
		this.airline = in.readUTF();
		this.origin = in.readUTF();
		this.originTerminal = in.readUTF();
		this.destination = in.readUTF();
		this.destinationTerminal = in.readUTF();
		this.departure = in.readLong();
		this.arrival = in.readLong();
	}
	
	@Override
	public String toString() {
		return airline + " " + origin + " " + originTerminal + " " + destination + " " + destinationTerminal + " " + departure + " " + arrival;
	}

	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((airline == null) ? 0 : airline.hashCode());
		result = prime * result + (int) (arrival ^ (arrival >>> 32));
		result = prime * result + (int) (departure ^ (departure >>> 32));
		result = prime * result
				+ ((destination == null) ? 0 : destination.hashCode());
		result = prime
				* result
				+ ((destinationTerminal == null) ? 0 : destinationTerminal
						.hashCode());
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
		result = prime * result
				+ ((originTerminal == null) ? 0 : originTerminal.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Flight other = (Flight) obj;
		if (airline == null) {
			if (other.airline != null)
				return false;
		} else if (!airline.equals(other.airline))
			return false;
		if (arrival != other.arrival)
			return false;
		if (departure != other.departure)
			return false;
		if (destination == null) {
			if (other.destination != null)
				return false;
		} else if (!destination.equals(other.destination))
			return false;
		if (destinationTerminal == null) {
			if (other.destinationTerminal != null)
				return false;
		} else if (!destinationTerminal.equals(other.destinationTerminal))
			return false;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
			return false;
		if (originTerminal == null) {
			if (other.originTerminal != null)
				return false;
		} else if (!originTerminal.equals(other.originTerminal))
			return false;
		return true;
	}

	@Override
	public Flight clone() throws CloneNotSupportedException {
		return new Flight(this.airline, this.origin, this.originTerminal, this.destination, this.destinationTerminal, 
				this.departure, this.arrival);
	}
	
	
	
}