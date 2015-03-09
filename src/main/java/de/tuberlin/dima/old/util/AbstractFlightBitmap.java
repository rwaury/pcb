package de.tuberlin.dima.old.util;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

@SuppressWarnings("serial")
public abstract class AbstractFlightBitmap implements Value, Cloneable {
	
	protected long start = 0L;
	protected long end = 0L;
	
	protected TimeResolution resolution;
	
	public abstract void setFlights(long[] timestamps);
	
	public abstract void addFlight(long timestamp);
	public abstract void removeFlight(long timestamp);
	
	public abstract boolean isSetInRange(long start, long end);
	public abstract boolean isSet(long timestamp);
	
	public abstract long[] getFirstNHits(long start, long end, int n);
	
	public abstract long getFirstTimestamp();
	public abstract long getLastTimeStamp();
	
	public abstract void removeUntil(long newStart);
	public abstract void extendUntil(long newEnd);
	
	public abstract int getNumBits();
	
	public abstract void write(DataOutputView out) throws IOException;

	public abstract void read(DataInputView in) throws IOException;
	
	public abstract AbstractFlightBitmap clone() throws CloneNotSupportedException;
}
