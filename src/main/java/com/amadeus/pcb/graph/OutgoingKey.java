package com.amadeus.pcb.graph;

public class OutgoingKey {
	
	private String destination = null;
	private long departureTimestamp = 0L;
	
	public OutgoingKey(String destination, long timestamp) {
		this.destination = destination;
		this.departureTimestamp = timestamp;
	}
	
	public String getDestination() {
		return destination;
	}

	public long getDepartureTimestamp() {
		return departureTimestamp;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ (int) (departureTimestamp ^ (departureTimestamp >>> 32));
		result = prime * result
				+ ((destination == null) ? 0 : destination.hashCode());
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
		OutgoingKey other = (OutgoingKey) obj;
		if (departureTimestamp != other.departureTimestamp)
			return false;
		if (destination == null) {
			if (other.destination != null)
				return false;
		} else if (!destination.equals(other.destination))
			return false;
		return true;
	}
}
