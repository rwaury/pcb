package de.tuberlin.dima.ti.analysis;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

// use trained models to assign OD pax to OD itineraries
public class PSCFTrafficEstimator extends RichCoGroupFunction<Itinerary, Tuple5<String, String, String, Double, PSLOptimizable>, Itinerary> {

    private Logit logit;

    private double BETA;

    private HashMap<String, AirportInfo> airports;

    public PSCFTrafficEstimator(Logit logit, double beta) {
        this.logit = logit;
        this.BETA = beta;
    }

    @Override
    public void open(Configuration parameters) {
        Collection<Tuple8<String, String, String, String, String, Double, Double, String>> broadcastSet =
                this.getRuntimeContext().getBroadcastVariable(TrafficAnalysis.AP_GEO_DATA);
        this.airports = new HashMap<String, AirportInfo>(broadcastSet.size());
        for(Tuple8<String, String, String, String, String, Double, Double, String> tuple8 : broadcastSet) {
            this.airports.put(tuple8.f0, new AirportInfo(tuple8));
        }
    }


    @Override
    public void coGroup(Iterable<Itinerary> connections, Iterable<Tuple5<String, String, String, Double, PSLOptimizable>> logitModel,
                        Collector<Itinerary> out) throws Exception {
        Iterator<Tuple5<String, String, String, Double, PSLOptimizable>> logitIter = logitModel.iterator();
        if(!logitIter.hasNext()) {
            return; // no model
        }
        Tuple5<String, String, String, Double, PSLOptimizable> logit = logitIter.next();
        if(logitIter.hasNext()) {
            throw new Exception("More than one logit model: " + logitIter.next().toString());
        }
        double estimate = logit.f3;
        double[] weights = logit.f4.asArray();
        Iterator<Itinerary> connIter = connections.iterator();
        if(!connIter.hasNext()) {
            return; // no connections
        }
        ArrayList<Double> PS = new ArrayList<Double>(); // TODO: compute PS/CF values
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
        for(int i = 0; i < itineraries.size(); i++) {
            softmaxSum += Math.exp(PSLOptimizable.linearPredictorFunction(LogitOptimizable.toArray(itineraries.get(i), minTime), weights, PS.get(i), BETA));
        }
        for(int i = 0; i < itineraries.size(); i++) {
            Itinerary e = itineraries.get(i);
            double itineraryEstimate = PSLOptimizable.softmaxPSCF(e, softmaxSum, weights, minTime, PS.get(i), BETA)*estimateWithoutMIDT;
            double fullEstimate = itineraryEstimate + e.f13.doubleValue();
            fullEstimate = Math.min(fullEstimate, e.f14.doubleValue()); // enforce lower and upper bound on itinerary level
            if(fullEstimate > 0) {
                e.f15 = fullEstimate;
                e.f16 = itineraryEstimate;
                e.f17 = estimate;
                out.collect(e);
            }
        }

    }

    private class AirportInfo {

        String iata;
        String city;
        String state;
        String country;
        String region;
        double latitude;
        double longitude;
        String icao;

        public AirportInfo(Tuple8<String, String, String, String, String, Double, Double, String> tuple8) {
            this.iata = tuple8.f0;
            this.city = tuple8.f1;
            this.state = tuple8.f2;
            this.country = tuple8.f3;
            this.region = tuple8.f4;
            this.latitude = tuple8.f5;
            this.longitude = tuple8.f6;
            this.icao = tuple8.f7;
        }
    }
}
