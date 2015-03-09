package de.tuberlin.dima.old.util;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Uncompressed bitmap that stores the data in a byte array
 */
@SuppressWarnings("serial")
public class VanillaBitmap extends AbstractFlightBitmap {
	
	private byte[] arr = null;
	
	private long resolutionDivisor = 0;
	
	private static final TimeResolution DEFAULT_RESOLUTION = TimeResolution.MINUTE;
	
	/**
	 * @param start first timestamp contained in bitmap
	 * @param end last timestamp contained in bitmap
	 * @param resolution granularity of timestamps
	 */
	public VanillaBitmap(long start, long end, TimeResolution resolution) {
		this(start, end, resolution, false);
	}
	
	/**
	 * @param start first timestamp contained in bitmap
	 * @param end last timestamp contained in bitmap
	 * @param resolution granularity of timestamps
	 */
	public VanillaBitmap(long start, long end, TimeResolution resolution, boolean forClone) {
		this.resolution = resolution;
		switch(this.resolution) {
			case MINUTE:
				this.resolutionDivisor = 1000L*60L;
				break;
			case HALFHOUR:
				this.resolutionDivisor = 1000L*60L*30L;
				break;
			case HOUR:
				this.resolutionDivisor = 1000L*60L*60L;
				break;
			case AMPM:
				this.resolutionDivisor = 1000L*60L*60L*12L;
				break;
			case DAY:
				this.resolutionDivisor = 1000L*60L*60L*24L;
				break;
		}
		this.start = (start/this.resolutionDivisor)*this.resolutionDivisor;
		this.end = (end/this.resolutionDivisor)*this.resolutionDivisor;
		if(!forClone) {
			long range = this.end - this.start + this.resolutionDivisor; // range includes end
			int bitCount = (int) (range/this.resolutionDivisor);
			int arrSize = (bitCount + Byte.SIZE-1)/Byte.SIZE;
			this.arr = new byte[arrSize];
		} else {
			this.arr = null;
		}
	}

	public VanillaBitmap() {
		this.start = 0L;
		this.end = 0L;
		this.resolution = DEFAULT_RESOLUTION;
	}
	
	@Override
	public void setFlights(long[] timestamps) {
		if(timestamps == null)
			return;
		for (int i = 0; i < timestamps.length; i++) {
			this.addFlight(timestamps[i]);
		}
	}

	@Override
	public void addFlight(long timestamp) {
		long rounded = (timestamp/this.resolutionDivisor)*this.resolutionDivisor;
		if(rounded < this.start || rounded > this.end)
			return;
		long offset = rounded-this.start;
		int index = (int) (offset/this.resolutionDivisor);
		this.arr[index/Byte.SIZE] |= (1 << (index % Byte.SIZE));
	}

	@Override
	public void removeFlight(long timestamp) {
		long rounded = (timestamp/this.resolutionDivisor)*this.resolutionDivisor;
		if(rounded < this.start || rounded > this.end)
			return;
		long offset = rounded-this.start;
		int index = (int) (offset/this.resolutionDivisor);
		this.arr[index/Byte.SIZE] &= ~(1 << (index % Byte.SIZE));
	}
	
	public boolean isSet(long timestamp) {
		long rounded = (timestamp/this.resolutionDivisor)*this.resolutionDivisor;
		if(rounded < this.start || rounded > this.end)
			return false;
		long offset = rounded-this.start;
		int index = (int) (offset/this.resolutionDivisor);
		byte result = (byte) (this.arr[index/Byte.SIZE] & (0x01 << (index % Byte.SIZE)));
		return (result != 0x00);
	}

	@Override
	public boolean isSetInRange(long start, long end) {
		long roundedStart = (start/this.resolutionDivisor)*this.resolutionDivisor;
		long roundedEnd = (end/this.resolutionDivisor)*this.resolutionDivisor;
		if((start > end) || (roundedStart > this.end) || (roundedEnd < this.start))
			return false;
		int startOffset = (int) ((roundedStart-this.start)/this.resolutionDivisor);
		int endOffset = (int) ((roundedEnd-this.start)/this.resolutionDivisor);
		for(int i = startOffset; i <= endOffset; i++) {
			if((this.arr[i/Byte.SIZE] & (0x01 << (i % Byte.SIZE))) != 0) {
				return true;
			}
		}
		return false;
	}

	@Override
	public long[] getFirstNHits(long start, long end, int n) {
		long roundedStart = (start/this.resolutionDivisor)*this.resolutionDivisor;
		long roundedEnd = (end/this.resolutionDivisor)*this.resolutionDivisor;
		if((start > end) || (roundedStart > this.end) || (roundedEnd < this.start) || n <= 0)
			return null;
		if(roundedStart < this.start)
			roundedStart = this.start;
		if(roundedEnd > this.end)
			roundedEnd = this.end;
		int startOffset = (int) ((roundedStart-this.start)/this.resolutionDivisor);
		int endOffset = (int) ((roundedEnd-this.start)/this.resolutionDivisor);
		long[] result = new long[n];
		int found = 0;
		for(int i = startOffset; i <= endOffset ; i++) {
			if((this.arr[i/Byte.SIZE] & (0x01 << (i % Byte.SIZE))) != 0) {
				result[found] = (i*this.resolutionDivisor)+this.start;
				found++;
				if(found==n)
					break;
			}
		}
		if (found == 0)
			return null;
		if(found < n) {
			long[] newResult = new long[found];
			System.arraycopy(result, 0, newResult, 0, found);
			return newResult;
		} else {
			return result;
		}
	}

	@Override
	public long getFirstTimestamp() {
		return start;
	}

	@Override
	public long getLastTimeStamp() {
		return end;
	}

	@Override
	public void removeUntil(long newStart) {
		// TODO Auto-generated method stub
	}

	@Override
	public void extendUntil(long newEnd) {
		// TODO Auto-generated method stub
	}

	@Override
	public int getNumBits() {
		return (int) ((this.end-this.start+this.resolutionDivisor)/this.resolutionDivisor);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(start);
		out.writeLong(end);
		out.writeInt(resolution.ordinal());
		out.writeInt(arr.length);
		out.write(arr);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.start = in.readLong();
		this.end = in.readLong();
		this.resolution = TimeResolution.values()[in.readInt()];
		switch(this.resolution) {
			case MINUTE:
				this.resolutionDivisor = 1000L*60L;
				break;
			case HALFHOUR:
				this.resolutionDivisor = 1000L*60L*30L;
				break;
			case HOUR:
				this.resolutionDivisor = 1000L*60L*60L;
				break;
			case AMPM:
				this.resolutionDivisor = 1000L*60L*60L*12L;
				break;
			case DAY:
				this.resolutionDivisor = 1000L*60L*60L*24L;
				break;
		}
		int arrSize = in.readInt();
		this.arr = new byte[arrSize];
		in.readFully(this.arr);
	}
	
	@Override
	public AbstractFlightBitmap clone() throws CloneNotSupportedException {
		VanillaBitmap clone = new VanillaBitmap(this.start, this.end, this.resolution, true);
		clone.arr = this.arr;
		return clone;
	}
}
