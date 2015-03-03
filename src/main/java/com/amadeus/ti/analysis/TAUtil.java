package com.amadeus.ti.analysis;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

public class TAUtil {

    public static double decayingFunction(double minTravelTime, double maxTravelTime, double avgTravelTime, double distance, double count, boolean isIntercontinental, boolean isInternational, boolean isInterstate) {
        double lnMode = 0.0;
        if(isIntercontinental) {
            lnMode = Math.log(5545.0); // JFK-LHR
        } else if(isInternational) {
            lnMode = Math.log(832.0); // HKG-TPE
        } else if(isInterstate) {
            lnMode = Math.log(451.0); // GMP-CJU
        } else {
            lnMode = Math.log(543.0); // SFO-LAX
        }
        double sigma2 = 2.0;
        double sigma = Math.sqrt(sigma2);
        double mu = lnMode + sigma2;
        double x = distance;
        double numerator = Math.exp(-1.0*(Math.pow(Math.log(x)-mu, 2.0)/(2.0*sigma2)));
        double denominator = x*sigma*Math.sqrt(2.0*Math.PI);
        return numerator/denominator;
        //return 1.0/Math.sqrt(minTravelTime*avgTravelTime*distance);
    }

    public static double mahalanobisDistance(ArrayRealVector x, ArrayRealVector y, Array2DRowRealMatrix Sinv) {
        ArrayRealVector xmy = x.subtract(y);
        RealVector SinvXmy = Sinv.operate(xmy);
        double result = xmy.dotProduct(SinvXmy);
        return Math.sqrt(result);
    }

    // Converts aggregated MIDT to Itinerary with estimate
    public static Itinerary MIDTToItinerary(MIDT midt) {
        return new Itinerary(midt.f0, midt.f1, midt.f2, midt.f3, midt.f4, midt.f5, midt.f6, midt.f7, -1.0, -1.0, midt.f8, midt.f9, midt.f10, midt.f11, midt.f11, midt.f11, -1.0, -1.0, "MIDT", midt.f13);
    }
}
