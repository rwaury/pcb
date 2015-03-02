package com.amadeus.ti.analysis;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

// add lower bound to itineraries that have MIDT hits/ emit the rest without change
public class ItineraryMIDTMerger implements CoGroupFunction<Itinerary, MIDT, Itinerary> {

    @Override
    public void coGroup(Iterable<Itinerary> itineraries, Iterable<MIDT> midts, Collector<Itinerary> out) throws Exception {
        Iterator<Itinerary> itinIter = itineraries.iterator();
        Iterator<MIDT> midtIter = midts.iterator();
        if (!midtIter.hasNext()) {
            out.collect(itinIter.next());
        } else if (!itinIter.hasNext()) {
            out.collect(TAUtil.MIDTToItinerary(midtIter.next()));
        } else {
            MIDT midt = midtIter.next();
            Itinerary itin = itinIter.next();
            itin.f13 = midt.f11;
            out.collect(itin);
        }
        if (itinIter.hasNext()) {
            throw new Exception("More than one Itinerary: " + itinIter.next().toString());
        }
        if (midtIter.hasNext()) {
            throw new Exception("More than one MIDT: " + midtIter.next().toString());
        }

    }
}