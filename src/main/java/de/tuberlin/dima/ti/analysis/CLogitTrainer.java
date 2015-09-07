package de.tuberlin.dima.ti.analysis;

import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.optimize.OptimizationException;
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
public class CLogitTrainer extends RichGroupReduceFunction<MIDT, Tuple4<String, String, String, PSLOptimizable>> {

    private double BETA;

    private HashMap<String, AirportInfo> airports;

    public CLogitTrainer(double beta) {
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
        ArrayList<Double> CF = new ArrayList<Double>();
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
        }
        if(trainingData.size() < 2) {
            return;
        }
        PSLOptimizable optimizable = new PSLOptimizable(BETA);
        if(minTravelTime < 1) {
            minTravelTime = 1;
        }
        optimizable.setTrainingData(trainingData, minTravelTime, CF);
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
}