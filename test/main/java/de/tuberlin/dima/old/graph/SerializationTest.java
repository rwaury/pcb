package de.tuberlin.dima.old.graph;

import junit.framework.TestCase;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.io.network.serialization.DataInputDeserializer;
import org.apache.flink.runtime.io.network.serialization.DataOutputSerializer;
import org.junit.Test;

public class SerializationTest extends TestCase{
	
	@Test
	public void testFlightSerialization() {
		try {
			Flight a = new Flight("AA", "AAA", "BBB", 100000000L, 110000000L);
			Flight b = new Flight("AB", "AAB", "BBC", 100000002L, 110000002L);
			Flight c = new Flight("AC", "AAC", "BBD", 100000004L, 110000004L);
			DataOutputSerializer out = new DataOutputSerializer(1024);
			a.write(out);
			b.write(out);
			c.write(out);
			
			DataInputView in = new DataInputDeserializer(out.wrapAsByteBuffer());
			Flight a1 = new Flight();
			Flight b1 = new Flight();
			Flight c1 = new Flight();
			a1.read(in);
			b1.read(in);
			c1.read(in);
			assertTrue(a.equals(a1));
			assertTrue(b.equals(b1));
			assertTrue(c.equals(c1));
			assertFalse(a.equals(b1));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFlightConnectionSerialization() {
		try {
			FlightConnection ca = new FlightConnection();
			Flight fa = new Flight("AA", "AAA", "BBB", 100000000L, 110000000L);
			Flight fb = new Flight("AB", "AAB", "BBC", 100000002L, 110000002L);
			Flight fc = new Flight("AC", "AAC", "BBD", 100000004L, 110000004L);
			ca.addFlight(fa);
			ca.addFlight(fb);
			ca.addFlight(fc);
			FlightConnection cb = new FlightConnection();
			Flight fd = new Flight("AD", "AAD", "BBE", 100000000L, 110000000L);
			Flight fe = new Flight("AE", "AAE", "BBF", 100000002L, 110000002L);
			Flight ff = new Flight("AF", "AAF", "BBG", 100000004L, 110000004L);
			cb.addFlight(fd);
			cb.addFlight(fe);
			cb.addFlight(ff);
			FlightConnection cc = new FlightConnection();
			FlightConnection cd = new FlightConnection();
			Flight fg = new Flight("AC", "AAC", "BBD", 100000004L, 110000004L);
			cd.addFlight(fg);

			DataOutputSerializer out = new DataOutputSerializer(1024);
			ca.write(out);
			cb.write(out);
			cc.write(out);
			cd.write(out);
			
			DataInputView in = new DataInputDeserializer(out.wrapAsByteBuffer());
			FlightConnection ca1 = new FlightConnection();
			FlightConnection cb1 = new FlightConnection();
			FlightConnection cc1 = new FlightConnection();
			FlightConnection cd1 = new FlightConnection();
			ca1.read(in);
			cb1.read(in);
			cc1.read(in);
			cd1.read(in);

			assertTrue(ca.equals(ca1));
			assertTrue(cb.equals(cb1));
			assertTrue(cc.equals(cc1));
			assertTrue(cd.equals(cd1));
			assertFalse(ca.equals(cb1));
			assertFalse(cc.equals(ca1));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}
