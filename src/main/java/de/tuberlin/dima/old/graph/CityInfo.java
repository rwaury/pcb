package de.tuberlin.dima.old.graph;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Ignore for now
 */
@SuppressWarnings("serial")
public class CityInfo implements Value {
	
	String name = null;
	
	double latitude = 0.0;
	double longitude = 0.0;
	
	int UTCOffSetInMinutes = 0;
	
	ArrayList<AirportInfo> airports = null;
	
	public CityInfo() {}

	@Override
	public void write(DataOutputView out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void read(DataInputView in) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}