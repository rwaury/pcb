package de.tuberlin.dima.ti.analysis;

import de.tuberlin.dima.ti.pcb.CBUtil;
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
        HashMap<String, Integer> segmentUses = new HashMap<String, Integer>();
        ArrayList<PSValue> PSValues = new ArrayList<PSValue>();
        ArrayList<CFValue> CFValues = new ArrayList<CFValue>();
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
                if(this.logit == Logit.PSL) {
                    PSValue psv;
                    if(e.f20.isEmpty()) {
                        String seg1 = e.f0 + e.f1;
                        psv = new PSValue(seg1);
                        if(!segmentUses.containsKey(seg1)) {
                            segmentUses.put(seg1, 1);
                        } else {
                            int v = segmentUses.get(seg1);
                            segmentUses.put(seg1, v+1);
                        }
                    } else {
                        if(e.f21.isEmpty()) {
                            String seg1 = e.f0 + e.f20;
                            String seg2 = e.f20 + e.f1;
                            AirportInfo o = this.airports.get(e.f0);
                            AirportInfo h1 = this.airports.get(e.f20);
                            AirportInfo d = this.airports.get(e.f1);
                            double dist1 = CBUtil.dist(o.latitude, o.longitude, h1.latitude, h1.longitude);
                            double dist2 = CBUtil.dist(h1.latitude, h1.longitude, d.latitude, d.longitude);
                            psv = new PSValue(seg1, seg2, dist1/(dist1+dist2), dist2/(dist1+dist2));
                            if(!segmentUses.containsKey(seg1)) {
                                segmentUses.put(seg1, 1);
                            } else {
                                int v = segmentUses.get(seg1);
                                segmentUses.put(seg1, v+1);
                            }
                            if(!segmentUses.containsKey(seg2)) {
                                segmentUses.put(seg2, 1);
                            } else {
                                int v = segmentUses.get(seg2);
                                segmentUses.put(seg2, v+1);
                            }
                        } else {
                            String seg1 = e.f0 + e.f20;
                            String seg2 = e.f20 + e.f21;
                            String seg3 = e.f21 + e.f1;
                            AirportInfo o = this.airports.get(e.f0);
                            AirportInfo h1 = this.airports.get(e.f20);
                            AirportInfo h2 = this.airports.get(e.f21);
                            AirportInfo d = this.airports.get(e.f1);
                            double dist1 = CBUtil.dist(o.latitude, o.longitude, h1.latitude, h1.longitude);
                            double dist2 = CBUtil.dist(h1.latitude, h1.longitude, h2.latitude, h2.longitude);
                            double dist3 = CBUtil.dist(h2.latitude, h2.longitude, d.latitude, d.longitude);
                            psv = new PSValue(seg1, seg2, seg3, dist1/(dist1+dist2+dist3), dist2/(dist1+dist2+dist3), dist3/(dist1+dist2+dist3));
                            if(!segmentUses.containsKey(seg1)) {
                                segmentUses.put(seg1, 1);
                            } else {
                                int v = segmentUses.get(seg1);
                                segmentUses.put(seg1, v+1);
                            }
                            if(!segmentUses.containsKey(seg2)) {
                                segmentUses.put(seg2, 1);
                            } else {
                                int v = segmentUses.get(seg2);
                                segmentUses.put(seg2, v+1);
                            }
                            if(!segmentUses.containsKey(seg3)) {
                                segmentUses.put(seg3, 1);
                            } else {
                                int v = segmentUses.get(seg3);
                                segmentUses.put(seg3, v+1);
                            }
                        }
                    }
                    PSValues.add(psv);
                } else  {
                    CFValue cfv;
                    if(e.f20.isEmpty()) {
                        String seg1 = e.f0 + e.f1;
                        AirportInfo o = this.airports.get(e.f0);
                        AirportInfo d = this.airports.get(e.f1);
                        double dist1 = CBUtil.dist(o.latitude, o.longitude, d.latitude, d.longitude);
                        cfv = new CFValue(seg1, dist1);
                    } else {
                        if(e.f21.isEmpty()) {
                            String seg1 = e.f0 + e.f20;
                            String seg2 = e.f20 + e.f1;
                            AirportInfo o = this.airports.get(e.f0);
                            AirportInfo h1 = this.airports.get(e.f20);
                            AirportInfo d = this.airports.get(e.f1);
                            double dist1 = CBUtil.dist(o.latitude, o.longitude, h1.latitude, h1.longitude);
                            double dist2 = CBUtil.dist(h1.latitude, h1.longitude, d.latitude, d.longitude);
                            cfv = new CFValue(seg1, seg2, dist1, dist2);
                        } else {
                            String seg1 = e.f0 + e.f20;
                            String seg2 = e.f20 + e.f21;
                            String seg3 = e.f21 + e.f1;
                            AirportInfo o = this.airports.get(e.f0);
                            AirportInfo h1 = this.airports.get(e.f20);
                            AirportInfo h2 = this.airports.get(e.f21);
                            AirportInfo d = this.airports.get(e.f1);
                            double dist1 = CBUtil.dist(o.latitude, o.longitude, h1.latitude, h1.longitude);
                            double dist2 = CBUtil.dist(h1.latitude, h1.longitude, h2.latitude, h2.longitude);
                            double dist3 = CBUtil.dist(h2.latitude, h2.longitude, d.latitude, d.longitude);
                            cfv = new CFValue(seg1, seg2, seg3, dist1, dist2, dist3);
                        }
                    }
                    CFValues.add(cfv);
                }
            }
            count++;
        }
        double estimateWithoutMIDT = estimate - (double) MIDTonlyPax;
        estimateWithoutMIDT -= (double) lowerBoundSum;
        if(minTime < 1) {
            minTime = 1;
        }
        ArrayList<Double> PS = new ArrayList<Double>(); // TODO: compute PS/CF values
        if(this.logit == Logit.PSL) {
            for(PSValue psv : PSValues) {
                double ps = 0.0;
                double segUses = 1.0;
                for(int i = 0; i < psv.size(); i++) {
                    segUses = (double) segmentUses.get(psv.segments[i]);
                    ps += psv.segmentShares[i]*(1.0/segUses);
                }
                PS.add(Math.log(ps));
            }
        } else {
            for (int k = 0; k < CFValues.size(); k++) {
                CFValue cfk = CFValues.get(k);
                double Lk = cfk.distance();
                double sum = 0.0;
                for (int l = 0; l < CFValues.size(); l++) {
                    if(k == l) {
                        continue;
                    }
                    CFValue cfl = CFValues.get(l);
                    double Ll = cfl.distance();
                    double Lkl = cfk.sharedDist(cfl);
                    sum += (Lkl/Math.sqrt(Lk*Ll))*((Lk-Lkl)/(Ll-Lkl));
                }
                double cf = Math.log(1+sum);
                PS.add(cf);
            }
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

    private class PSValue {

        String[] segments;
        double[] segmentShares;

        public PSValue(String seg1) {
            this.segments = new String[]{seg1};
            this.segmentShares = new double[]{1.0};
        }

        public PSValue(String seg1, String seg2, double seg1Share, double seg2Share) {
            this.segments = new String[]{seg1, seg2};
            this.segmentShares = new double[]{seg1Share, seg2Share};
        }

        public PSValue(String seg1, String seg2, String seg3, double seg1Share, double seg2Share, double seg3Share) {
            this.segments = new String[]{seg1, seg2, seg3};
            this.segmentShares = new double[]{seg1Share, seg2Share, seg3Share};
        }

        public int size() {
            return this.segments.length;
        }
    }

    private class CFValue {

        String[] segments;
        double[] segmentDists;

        public CFValue(String seg1, double dist1) {
            this.segments = new String[]{seg1};
            this.segmentDists = new double[]{dist1};
        }

        public CFValue(String seg1, String seg2, double dist1, double dist2) {
            this.segments = new String[]{seg1, seg2};
            this.segmentDists = new double[]{dist1, dist2};
        }

        public CFValue(String seg1, String seg2, String seg3, double dist1, double dist2, double dist3) {
            this.segments = new String[]{seg1, seg2, seg3};
            this.segmentDists = new double[]{dist1, dist2, dist3};
        }

        public int size() {
            return this.segments.length;
        }

        public double distance() {
            double result = 0.0;
            for (int i = 0; i < segmentDists.length; i++) {
                result += segmentDists[i];
            }
            return result;
        }

        public double sharedDist(CFValue cfv) {
            if(cfv.size() == 1 && this.size() ==1) {
                return this.segmentDists[0];
            }
            double sharedDist = 0.0;
            for(int i = 0; i < cfv.size(); i++) {
                String seg = cfv.segments[i];
                for (int j = 0; j < this.size(); j++) {
                    if(this.segments[j].equals(seg)) {
                        sharedDist += this.segmentDists[j];
                    }
                }
            }
            return sharedDist;
        }

    }
}
