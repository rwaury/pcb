package de.tuberlin.dima.ti.analysis;

import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.optimize.OptimizationException;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

// train a logit model with the MIDT data
public class LogitTrainer implements GroupReduceFunction<MIDT, Tuple4<String, String, String, LogitOptimizable>> {

    @Override
    public void reduce(Iterable<MIDT> midts, Collector<Tuple4<String, String, String, LogitOptimizable>> out) throws Exception {
        ArrayList<LogitOptimizable.TrainingData> trainingData = new ArrayList<LogitOptimizable.TrainingData>();
        Iterator<MIDT> iterator = midts.iterator();
        MIDT midt = null;
        int minTravelTime = Integer.MAX_VALUE;
        while(iterator.hasNext()) {
            midt = iterator.next();
            if(midt.f8 < minTravelTime) {
                minTravelTime = midt.f8;
            }
            double percentageWaiting = (midt.f8 == 0 || midt.f9 == 0) ? 0.0 : midt.f9/midt.f8;
            trainingData.add(new LogitOptimizable.TrainingData(midt.f8, percentageWaiting, midt.f10, midt.f12, midt.f13, midt.getNumAirlines(), midt.f11));
        }
        if(trainingData.size() < 2) {
            return;
        }
        LogitOptimizable optimizable = new LogitOptimizable();
        if(minTravelTime < 1) {
            minTravelTime = 1;
        }
        optimizable.setTrainingData(trainingData, minTravelTime);
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
        out.collect(new Tuple4<String, String, String, LogitOptimizable>(midt.f0, midt.f1, midt.f2, optimizable));
    }
}