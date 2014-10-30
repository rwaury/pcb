package com.amadeus.pcb.graph;

import com.amadeus.pcb.util.AbstractFlightBitmap;
import com.amadeus.pcb.util.VanillaBitmap;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

/**
* Class representing a connection between two airports
* 
* Assumptions:
* 	- 	at any given minute (or even coarser granularity if chosen) at most on flight 
* 		will be scheduled on an airport-to-airport connection e.g. LH flight TXL-FRA 
* 		and AB flight TXL-FRA both leaving at the same time will only be recognized 
* 		as a single flight in the internal bitmap representation and removing one of 
* 		the flights will in fact remove both
*  -	flight insertions outside the given time range will fail silently
*
*/
@SuppressWarnings("serial")
public class ConnectionInfo implements Value, Cloneable {
	
	private double destinationLatitude = 0.0;
	private double destinationLongitude = 0.0;
	
	private AbstractFlightBitmap bitmap = null;
	
	public ConnectionInfo() {}

	public double getDestinationLatitude() {
		return destinationLatitude;
	}

	public void setDestinationLatitude(double destinationLatitude) {
		this.destinationLatitude = destinationLatitude;
	}

	public double getDestinationLongitude() {
		return destinationLongitude;
	}

	public void setDestinationLongitude(double destinationLongitude) {
		this.destinationLongitude = destinationLongitude;
	}

	public AbstractFlightBitmap getBitmap() {
		return bitmap;
	}

	public void setBitmap(AbstractFlightBitmap bitmap) {
		this.bitmap = bitmap;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeDouble(destinationLatitude);
		out.writeDouble(destinationLongitude);
		bitmap.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.destinationLatitude = in.readDouble();
		this.destinationLongitude = in.readDouble();
		this.bitmap = new VanillaBitmap();
		bitmap.read(in);
	}
	
	@Override
	public String toString() {
		return destinationLatitude + " " + destinationLongitude;
	}

	@Override
	public ConnectionInfo clone() throws CloneNotSupportedException {
		ConnectionInfo clone = new ConnectionInfo();
		clone.setDestinationLatitude(this.destinationLatitude);
		clone.setDestinationLongitude(this.destinationLongitude);
		clone.setBitmap(this.bitmap.clone());
		return clone;
	}
	
	
	
}