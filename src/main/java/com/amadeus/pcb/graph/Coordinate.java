package com.amadeus.pcb.graph;

public class Coordinate {

	private double latitude = 0.0;
	private double longitude = 0.0;
	
	private String countryCode = null;

	public Coordinate(double latitude, double longitude, String countryCode) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.countryCode = countryCode;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public String getCountryCode() {
		return countryCode;
	}
	
}
