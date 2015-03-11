package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

// use trained models to assign OD pax to OD itineraries
public class TrafficEstimator implements CoGroupFunction<Itinerary, Tuple5<String, String, String, Double, LogitOptimizable>, Itinerary> {

    @Override
    public void coGroup(Iterable<Itinerary> connections, Iterable<Tuple5<String, String, String, Double, LogitOptimizable>> logitModel,
                        Collector<Itinerary> out) throws Exception {
        Iterator<Tuple5<String, String, String, Double, LogitOptimizable>> logitIter = logitModel.iterator();
        if(!logitIter.hasNext()) {
            return; // no model
        }
        Tuple5<String, String, String, Double, LogitOptimizable> logit = logitIter.next();
        if(logitIter.hasNext()) {
            throw new Exception("More than one logit model: " + logitIter.next().toString());
        }
        double estimate = logit.f3;
        double[] weights = logit.f4.asArray();
        Iterator<Itinerary> connIter = connections.iterator();
        if(!connIter.hasNext()) {
            return; // no connections
        }
        int count = 0;
        int MIDTonlyPax = 0;
        ArrayList<Itinerary> itineraries = new ArrayList<Itinerary>();
        int minTime = Integer.MAX_VALUE;
        int lowerBoundSum = 0;
        while (connIter.hasNext()) {
            Itinerary e = connIter.next().deepCopy();
            if(e.f18.equals("MIDT")) {
                MIDTonlyPax += e.f13;
                e.f16 = (double)e.f13;
                e.f17 = estimate;
                out.collect(e);
            } else {
                itineraries.add(e);
                if (e.f10 < minTime) {
                    minTime = e.f10;
                }
                lowerBoundSum += e.f13;
            }
            count++;
        }
        double estimateWithoutMIDT = estimate - (double) MIDTonlyPax;
        estimateWithoutMIDT -= (double) lowerBoundSum;
        if(minTime < 1) {
            minTime = 1;
        }
        double softmaxSum = 0.0;
        for(Itinerary e : itineraries) {
            softmaxSum += Math.exp(LogitOptimizable.linearPredictorFunction(LogitOptimizable.toArray(e, minTime), weights));
        }
        for(Itinerary e : itineraries) {
            double itineraryEstimate = LogitOptimizable.softmax(e, softmaxSum, weights, minTime)*estimateWithoutMIDT;
            int roundedEstimate = (int)Math.round(itineraryEstimate) + e.f13;
            roundedEstimate = Math.min(roundedEstimate, e.f14); // enforce lower and upper bound on itinerary level
            if(roundedEstimate > 0) {
                e.f15 = roundedEstimate;
                e.f16 = itineraryEstimate;
                e.f17 = estimate;
                out.collect(e);
            }
        }

    }
}