package de.tuberlin.dima.old.graph;

import de.tuberlin.dima.old.spargel.AirportGraphComputation;
import org.apache.commons.lang3.exception.CloneFailedException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.ArrayList;

@SuppressWarnings("serial")
public class FlightConnection implements Value, Cloneable {
	
	private ArrayList<Flight> flights = null;
	
	public FlightConnection() {}
	
	public FlightConnection(Flight f) {
		this.flights = new ArrayList<Flight>(1);
		this.flights.add(f);
	}
	
	public FlightConnection(FlightConnection conn) {
		if(conn != null && conn.getSize() > 0) {
			this.flights = new ArrayList<Flight>(conn.getSize());
			for (Flight f : conn.getFlights()) {
				this.flights.add(f);
			}
		}
	}
	
	public void addFlight(Flight f) throws Exception {
		if(f == null) {
			throw new Exception("You cannot add nulls to a connection: " + toString() );
		}
		if(this.flights != null) {
			this.flights.add(f);
		} else {
			this.flights = new ArrayList<Flight>(1);
			this.flights.add(f);
		}
	}

	public ArrayList<Flight> getFlights() {
		return flights;
	}
	
	public Flight getLastFlight() {
		if(this.flights == null || this.flights.size() == 0)
			return null;
		return flights.get(this.flights.size()-1);
	}
	
	public Flight getFirstFlight() {
		if(this.flights == null || this.flights.size() == 0)
			return null;
		return flights.get(0);
	}
	
	public int getSize() {
		if(this.flights != null) {
			return this.flights.size();
		} else {
			return 0;
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		assert AirportGraphComputation.MAX_LEGS <= Byte.MAX_VALUE;
		out.write(getSize());
		if(flights != null) {
			for (Flight flight : flights) {
				flight.write(out);
			}
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		int numFlights = (int) in.readByte();
		if(numFlights > 0) {
			this.flights = new ArrayList<Flight>(numFlights);
			for (int i = 0; i < numFlights; i++) {
				Flight f = new Flight();
				f.read(in);
				flights.add(f);
			}
		} else {
			this.flights = null;
		}
	}
	
	@Override
	public String toString() {
		String result = "";
		for (Flight f : flights) {
			result += f.toString() + "/";
		}
		return result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		for (Flight flight : flights) {
			result = prime * result + flight.hashCode();
		}
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
		FlightConnection other = (FlightConnection) obj;
		if (this.flights == null && other.flights != null)
			return false;
		if (this.flights != null && other.flights == null)
			return false;
		if (this.flights == null && other.flights == null)
			return true;
		if (this.flights.size() != other.getSize())
			return false;
		int i = 0;
		for (Flight flight : flights) {
			if(!flight.equals(other.flights.get(i)))
				return false;
			i++;
		}
		return true;
	}

	@Override
	public FlightConnection clone() throws CloneNotSupportedException {
		FlightConnection clone = new FlightConnection();
		for (Flight flight : this.flights) {
			try {
				clone.addFlight(flight.clone());
			} catch (Exception e) {
				throw new CloneFailedException(e.getMessage());
			}
		}
		return clone;
	}
	
	
	
}