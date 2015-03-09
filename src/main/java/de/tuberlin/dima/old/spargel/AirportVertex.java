package de.tuberlin.dima.old.spargel;

import de.tuberlin.dima.old.graph.AirportInfo;
import de.tuberlin.dima.old.graph.FlightConnection;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.VertexUpdateFunction;

@SuppressWarnings("serial")
public class AirportVertex extends VertexUpdateFunction<String, AirportInfo, FlightConnection> {

	@Override
	public void updateVertex(String vertexKey, AirportInfo vertexValue,
			MessageIterator<FlightConnection> inMessages) throws Exception {
		if(!inMessages.hasNext() && getSuperstepNumber() > 1) {
			return;
		}
		//if(vertexValue.getOutgoingFlights() != null) {
		//	throw new Exception("outgoing expected to be null! " + vertexKey + " " + vertexValue.getOutgoingFlights().get(0).toString());
		//}
		AirportInfo newValue = new AirportInfo(vertexValue.getLatitude(), vertexValue.getLongitude(), vertexValue.getHashMap(), vertexValue.getArrived());
		newValue.setOutgoingOffset(newValue.getArrivedSize());
		for (FlightConnection conn : inMessages) {
			if(!conn.getLastFlight().getDestination().equals(vertexKey))
				throw new Exception("Received wrong flight: " + conn.toString());
			newValue.addArrivedFlight(conn.clone()); // arrived flights are part of the end result
			//newValue.addOutgoingFlight(conn.clone()); // send out during the next call to sendMessages()
		}
		this.setNewVertexValue(newValue);
	}
}