package com.amadeus.pcb.util;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class VanillaBitmapTest extends TestCase {
	
	private static final long seed = 5883939292929292191L;
	Random random = new Random(seed);
	
	private static final long minute = 60L * 1000L;
	private static final long hour = 60L * minute;
	private static final long day = 24L * hour;
	
	private static final long start = 1398902400000L; // Thu, 01 May 2014 00:00:00 GMT
	private static final long end = 1430438400000L;//1399593540000L; // Thu, 08 May 2014 23:59:00 GMT
	private static final long range = end-start+minute; // from start up to including end
	
	private static final int NUM_ENTRIES = 25000000;
	
	@Test
	public void testInsertAndRetrieve() {
		VanillaBitmap bitmap = new VanillaBitmap(start, end, TimeResolution.MINUTE);
		long[] timestamps = new long[NUM_ENTRIES];
		long t = 0L;
		long l = 0L;
		for (int i = 0; i < NUM_ENTRIES; i++) {
			l = Math.abs(random.nextLong());
			t = (l/minute) * minute;
			t %= range;
			t += start;
			timestamps[i] = t;
			bitmap.addFlight(timestamps[i]);
			assertTrue(bitmap.isSet(timestamps[i]));
		}
		HashSet<Long> inserted = new HashSet<Long>(NUM_ENTRIES);
		for (int i = 0; i < timestamps.length; i++) {
			inserted.add(timestamps[i]);
		}
		long[] retrievedA = bitmap.getFirstNHits(start, end, NUM_ENTRIES);
		HashSet<Long> retrieved = new HashSet<Long>(NUM_ENTRIES);
		for (int i = 0; i < retrievedA.length; i++) {
			retrieved.add(retrievedA[i]);
		}
		assertTrue(inserted.size() == retrieved.size());
		assertTrue(retrieved.containsAll(inserted));
	}
	
	@Test
	public void testInsertAndDelete() {
		VanillaBitmap bitmap = new VanillaBitmap(start, end, TimeResolution.MINUTE);
		long t = 0L;
		long l = 0L;
		for (int i = 0; i < NUM_ENTRIES; i++) {
			l = Math.abs(random.nextLong());
			t = (l/minute) * minute;
			t %= range;
			t += start;
			bitmap.addFlight(t);
			assertTrue(bitmap.isSet(t));
			assertFalse(bitmap.isSet(t + minute));
			assertFalse(bitmap.isSet(t - minute));
			assertFalse(bitmap.isSet(t + day));
			assertFalse(bitmap.isSet(t - day));
			bitmap.removeFlight(t);
			assertFalse(bitmap.isSet(t));
		}
		bitmap.addFlight(start);
		bitmap.addFlight(end);
		assertTrue(bitmap.isSet(start));
		assertTrue(bitmap.isSet(end));
		assertTrue(bitmap.isSetInRange(start, end));
		bitmap.removeFlight(start);
		bitmap.removeFlight(end);
		assertFalse(bitmap.isSet(start));
		assertFalse(bitmap.isSet(end));
		assertNull(bitmap.getFirstNHits(start, end, NUM_ENTRIES));
		assertFalse(bitmap.isSetInRange(start, end));
	}
	
	@Test
	public void testResize() {
		fail("nyi");
	}
	
	@Test
	public void testRangeQuery() {
		VanillaBitmap bitmap = new VanillaBitmap(start, end, TimeResolution.MINUTE);
		long[] timestamps = new long[NUM_ENTRIES];
		long t = 0L;
		long l = 0L;
		for (int i = 0; i < NUM_ENTRIES; i++) {
			l = Math.abs(random.nextLong());
			t = (l/minute) * minute;
			t %= range;
			t += start;
			timestamps[i] = t;
			bitmap.addFlight(timestamps[i]);
			assertTrue(bitmap.isSet(timestamps[i]));
		}
		assertTrue(bitmap.isSetInRange(start, end));
		assertTrue(bitmap.isSetInRange(start+day, end-day));
		for (int i = 0; i < NUM_ENTRIES; i++) {
			bitmap.removeFlight(timestamps[i]);
			assertFalse(bitmap.isSet(timestamps[i]));
		}
		assertFalse(bitmap.isSetInRange(start, end));
	}
	
	@Test
	public void testBatchInsert() {
		VanillaBitmap bitmap = new VanillaBitmap(start, end, TimeResolution.MINUTE);
		long[] timestamps = new long[NUM_ENTRIES];
		long t = 0L;
		long l = 0L;
		for (int i = 0; i < NUM_ENTRIES; i++) {
			l = Math.abs(random.nextLong());
			t = (l/minute) * minute;
			t %= range;
			t += start;
			timestamps[i] = t;
		}
		bitmap.setFlights(timestamps);
		HashSet<Long> inserted = new HashSet<Long>(NUM_ENTRIES);
		for (int i = 0; i < timestamps.length; i++) {
			inserted.add(timestamps[i]);
		}
		long[] retrievedA = bitmap.getFirstNHits(start, end, NUM_ENTRIES);
		HashSet<Long> retrieved = new HashSet<Long>(NUM_ENTRIES);
		for (int i = 0; i < retrievedA.length; i++) {
			retrieved.add(retrievedA[i]);
		}
		assertTrue(inserted.size() == retrieved.size());
		assertTrue(retrieved.containsAll(inserted));
	}
	
	@Test
	public void testRangeRetrieve() {
		VanillaBitmap bitmap = new VanillaBitmap(start, end, TimeResolution.MINUTE);
		long[] input = {
				start, start+hour, start+hour+minute, start+(2*day+3*hour)-minute, 
				end-day-day, end-day-hour-minute, end-day, end-hour, end-minute,
				end
				};
		for (int i = 0; i < input.length; i++) {
			bitmap.addFlight(input[i]);
		}
		long[] timestamps = bitmap.getFirstNHits(start-day, end, input.length);
		assertTrue(timestamps.length == input.length);
		assertArrayEquals(input, timestamps);
		assertTrue(bitmap.getFirstNHits(start+hour+minute, end-hour, input.length).length == 6);
		assertTrue(bitmap.getFirstNHits(end, end, input.length).length == 1);
		assertTrue(bitmap.getFirstNHits(start, start, input.length).length == 1);
	}
	
	@Test
	public void testInfoFunctions() {
		fail("nyi");
	}

}
