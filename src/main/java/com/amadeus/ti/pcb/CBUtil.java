package com.amadeus.ti.pcb;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class CBUtil {

    public static final Character EMPTY = new Character(' ');


    /** capacity **/

    /**
     * @param conn two-leg connection
     * @return smallest capacity in connection
     */
    public static int getMinCap(Tuple2<Flight, Flight> conn) {
        if (conn.f0.getMaxCapacity() < conn.f1.getMaxCapacity()) {
            return conn.f0.getMaxCapacity();
        } else {
            return conn.f1.getMaxCapacity();
        }
    }

    /**
     * @param conn three-leg connection
     * @return smallest capacity in connection
     */
    public static int getMinCap(Tuple3<Flight, Flight, Flight> conn) {
        if (conn.f0.getMaxCapacity() <= conn.f1.getMaxCapacity() && conn.f0.getMaxCapacity() <= conn.f2.getMaxCapacity()) {
            return conn.f0.getMaxCapacity();
        } else if (conn.f1.getMaxCapacity() <= conn.f0.getMaxCapacity() && conn.f1.getMaxCapacity() <= conn.f2.getMaxCapacity()) {
            return conn.f1.getMaxCapacity();
        } else {
            return conn.f2.getMaxCapacity();
        }
    }


    /** detour **/

    /**
     * checks geoDetour for a two-leg flight
     *
     * @param originLatitude
     * @param originLongitude
     * @param hubLatitude
     * @param hubLongitude
     * @param destLatitude
     * @param destLongitude
     * @return true if acceptable, otherwise false
     * @throws Exception
     */
    public static boolean geoDetourAcceptable(double originLatitude, double originLongitude,
                                               double hubLatitude, double hubLongitude,
                                               double destLatitude, double destLongitude) throws Exception {
        double ODDist = dist(originLatitude, originLongitude, destLatitude, destLongitude);
        double legDistanceSum = 0.0;
        legDistanceSum += dist(originLatitude, originLongitude, hubLatitude, hubLongitude);
        legDistanceSum += dist(hubLatitude, hubLongitude, destLatitude, destLongitude);
        return (legDistanceSum / ODDist) <= computeMaxGeoDetour(ODDist, false);
    }

    /**
     * checks geoDetour for a three-leg flight
     *
     * @param originLatitude
     * @param originLongitude
     * @param hub1Latitude
     * @param hub1Longitude
     * @param hub2Latitude
     * @param hub2Longitude
     * @param destLatitude
     * @param destLongitude
     * @return true if acceptable, otherwise false
     * @throws Exception
     */
    public static boolean geoDetourAcceptable(double originLatitude, double originLongitude,
                                               double hub1Latitude, double hub1Longitude,
                                               double hub2Latitude, double hub2Longitude,
                                               double destLatitude, double destLongitude) throws Exception {
        double ODDist = dist(originLatitude, originLongitude, destLatitude, destLongitude);
        double legDistanceSum = 0.0;
        legDistanceSum += dist(originLatitude, originLongitude, hub1Latitude, hub1Longitude);
        legDistanceSum += dist(hub1Latitude, hub1Longitude, hub2Latitude, hub2Longitude);
        legDistanceSum += dist(hub2Latitude, hub2Longitude, destLatitude, destLongitude);
        return (legDistanceSum / ODDist) <= computeMaxGeoDetour(ODDist, true);
    }

    private static double computeMaxGeoDetour(double ODDist, boolean isThreeLeg) {
        if(isThreeLeg) {
            return Math.min(ParallelConnectionBuilder.MAX_DETOUR_THREE_LEG, -0.365 * Math.log(ODDist) + 4.8);
        } else {
            return Math.min(ParallelConnectionBuilder.MAX_DETOUR_TWO_LEG, -0.365 * Math.log(ODDist) + 4.8);
        }
    }


    /** MCT **/

    public static long computeMaxCT(double ODDist) {
        return (long) (180.0 * Math.log(ODDist + 1000.0) - 1000.0);
    }

    public static long computeMinCT() {
        return ParallelConnectionBuilder.MCT_MIN;
    }


    /** domestic/international **/

    public static boolean isDomestic(Flight f) {
        return f.getOriginCountry().equals(f.getDestinationCountry());
    }

    public static boolean isODDomestic(Flight in1, Flight in2) {
        return in1.getOriginCountry().equals(in2.getDestinationCountry());
    }

    public static boolean isFullyDomestic(Flight in1, Flight in2) {
        return in1.getOriginCountry().equals(in2.getDestinationCountry()) && in1.getDestinationCountry().equals(in1.getOriginCountry());
    }


    /** geographical distance **/

    /**
     * distance between two coordinates in kilometers
     */
    public static double dist(double lat1, double long1, double lat2, double long2) {
        final double d2r = Math.PI / 180.0;
        final double dlong = (long2 - long1) * d2r;
        final double dlat = (lat2 - lat1) * d2r;
        final double a = Math.pow(Math.sin(dlat / 2.0), 2.0)
                + Math.cos(lat1 * d2r)
                * Math.cos(lat2 * d2r)
                * Math.pow(Math.sin(dlong / 2.0), 2.0);
        final double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
        final double d = 6367.0 * c;
        return d;
    }

    public static double travelTimeAt100kphInMinutes(double ODDist) {
        return (ODDist * 60.0) / 100.0;
    }


    /** time windowing **/

    public static int getFirstWindow(long timestamp) {
        int windowIndex = (int) (2L * (timestamp - ParallelConnectionBuilder.START) / ParallelConnectionBuilder.WINDOW_SIZE);
        return windowIndex;
    }

    public static int getSecondWindow(long timestamp) {
        return getFirstWindow(timestamp) + 1;
    }
}
