package de.tuberlin.dima.ti.analysis;

import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.optimize.OptimizationException;
import de.tuberlin.dima.ti.pcb.CBUtil;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

// train a logit model with the MIDT data
public class PSLTrainer extends RichGroupReduceFunction<MIDT, Tuple4<String, String, String, PSLOptimizable>> {

    private double BETA;

    private HashMap<String, AirportInfo> airports;

    public PSLTrainer(double beta) {
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
    public void reduce(Iterable<MIDT> midts, Collector<Tuple4<String, String, String, PSLOptimizable>> out) throws Exception {
        ArrayList<PSLOptimizable.TrainingData> trainingData = new ArrayList<PSLOptimizable.TrainingData>();
        ArrayList<Double> PS = new ArrayList<Double>();
        ArrayList<PSValue> PSValues = new ArrayList<PSValue>();
        HashMap<String, Integer> segmentUses = new HashMap<String, Integer>();
        Iterator<MIDT> iterator = midts.iterator();
        MIDT midt = null;
        int minTravelTime = Integer.MAX_VALUE;
        while(iterator.hasNext()) {
            midt = iterator.next();
            if(midt.f8 < minTravelTime) {
                minTravelTime = midt.f8;
            }
            double percentageWaiting = (midt.f8 == 0 || midt.f9 == 0) ? 0.0 : midt.f9/midt.f8;
            trainingData.add(new PSLOptimizable.TrainingData(midt.f8, percentageWaiting, midt.f10, midt.f12, midt.f13, midt.getNumAirlines(), midt.f11));
            PSValue psv;
            if(midt.f14.isEmpty()) {
                String seg1 = midt.f0 + midt.f1;
                psv = new PSValue(seg1);
                if(!segmentUses.containsKey(seg1)) {
                    segmentUses.put(seg1, 1);
                } else {
                    int v = segmentUses.get(seg1);
                    segmentUses.put(seg1, v+1);
                }
            } else {
                if(midt.f15.isEmpty()) {
                    String seg1 = midt.f0 + midt.f14;
                    String seg2 = midt.f14 + midt.f1;
                    AirportInfo o = this.airports.get(midt.f0);
                    AirportInfo h1 = this.airports.get(midt.f14);
                    AirportInfo d = this.airports.get(midt.f1);
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
                    String seg1 = midt.f0 + midt.f14;
                    String seg2 = midt.f14 + midt.f15;
                    String seg3 = midt.f15 + midt.f1;
                    AirportInfo o = this.airports.get(midt.f0);
                    AirportInfo h1 = this.airports.get(midt.f14);
                    AirportInfo h2 = this.airports.get(midt.f15);
                    AirportInfo d = this.airports.get(midt.f1);
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
        }
        if(trainingData.size() < 2) {
            return;
        }
        for(PSValue psv : PSValues) {
            double ps = 0.0;
            double segUses = 1.0;
            for(int i = 0; i < psv.size(); i++) {
                segUses = (double) segmentUses.get(psv.segments[i]);
                ps += psv.segmentShares[i]*(1.0/segUses);
            }
            PS.add(Math.log(ps));
        }
        PSLOptimizable optimizable = new PSLOptimizable(BETA);
        if(minTravelTime < 1) {
            minTravelTime = 1;
        }
        optimizable.setTrainingData(trainingData, minTravelTime, PS);
        LimitedMemoryBFGS optimizer = new LimitedMemoryBFGS(optimizable);
        optimizer.setTolerance(TrafficAnalysis.OPTIMIZER_TOLERANCE);
        boolean converged = false;
        try {
            converged = optimizer.optimize(TrafficAnalysis.MAX_OPTIMIZER_ITERATIONS);
        } catch (IllegalArgumentException e) {
            // This exception may be thrown if L-BFGS
            // cannot step in the current direction.
            // This condition does not necessarily mean that
            // the optimizer has failed, but it doesn't want
            // to claim to have succeeded...
        } catch (OptimizationException o) {
            // see above
        } catch (Throwable t) {
            throw new Exception("Something went wrong in the optimizer. " + t.getMessage());
        }
        if(!converged) {
            return;
        }
        optimizable.clear(); // push the results in the tuple
        out.collect(new Tuple4<String, String, String, PSLOptimizable>(midt.f0, midt.f1, midt.f2, optimizable));
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
}