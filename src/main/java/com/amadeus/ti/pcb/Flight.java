package com.amadeus.ti.pcb;

import org.apache.flink.api.java.tuple.Tuple15;

/**
 * class containing a single flight connection (multiple connections may be merged from a multi-leg flight)
 */
public class Flight extends Tuple15<GeoInfo, Long, GeoInfo, Long,
        String, Integer, String, Integer, String, Character, GeoInfo, Integer, Integer, Integer, Integer> {

    public Flight() {
        super();
        this.f0 = new GeoInfo();
        this.f2 = new GeoInfo();
        this.f10 = new GeoInfo();
    }

    public Flight(String originAirport, String originTerminal, String originCity, String originState, String originCountry, String originRegion, Double originLatitude, Double originLongitude, String originICAO, Long departureTimestamp,
                  String destAirport, String destTerminal, String destCity, String destState, String destCountry, String destRegion, Double destLatitude, Double destLongitude, String destICAO, Long arrivalTimestamp,
                  String airline, Integer flightNumber, String aircraftType, Integer maxCapacity, String codeshareInfo, char trafficRestriction) {
        this.f0 = new GeoInfo(originAirport, originTerminal, originCity, originState, originCountry, originRegion, originLatitude, originLongitude, originICAO);
        this.f1 = departureTimestamp;
        this.f2 = new GeoInfo(destAirport, destTerminal, destCity, destState, destCountry, destRegion, destLatitude, destLongitude, destICAO);
        this.f3 = arrivalTimestamp;
        this.f4 = airline;
        this.f5 = flightNumber;
        this.f6 = aircraftType;
        this.f7 = maxCapacity;
        this.f8 = codeshareInfo;
        this.f9 = trafficRestriction;
        // the last GeoInfo field is redundant for most flights but required for merged multi-leg flights
        this.f10 = new GeoInfo(originAirport, originTerminal, originCity, originState, originCountry, originRegion, originLatitude, originLongitude, originICAO);
        this.f11 = 1; // number of legs
        this.f12 = CBUtil.getFirstWindow(departureTimestamp); // dep window
        this.f13 = CBUtil.getFirstWindow(arrivalTimestamp); // arr window 1
        this.f14 = CBUtil.getSecondWindow(arrivalTimestamp); // arr window 2
    }

    private Flight(Tuple15<GeoInfo, Long, GeoInfo, Long,
            String, Integer, String, Integer, String, Character, GeoInfo, Integer, Integer, Integer, Integer> tuple) {
        this.f0 = tuple.f0.clone();
        this.f1 = tuple.f1;
        this.f2 = tuple.f2.clone();
        this.f3 = tuple.f3;
        this.f4 = tuple.f4;
        this.f5 = tuple.f5;
        this.f6 = tuple.f6;
        this.f7 = tuple.f7;
        this.f8 = tuple.f8;
        this.f9 = tuple.f9;
        this.f10 = tuple.f10.clone();
        this.f11 = tuple.f11;
        this.f12 = tuple.f12;
        this.f13 = tuple.f13;
        this.f14 = tuple.f14;
    }

    public Flight clone() {
        return new Flight(this.copy());
    }

    public String getOriginAirport() {
        return this.f0.f0;
    }

    public void setOriginAirport(String originAirport) {
        this.f0.f0 = originAirport;
    }


    public String getOriginTerminal() {
        return this.f0.f1;
    }

    public void setOriginTerminal(String originTerminal) {
        this.f0.f1 = originTerminal;
    }


    public String getOriginCity() {
        return this.f0.f2;
    }

    public void setOriginCity(String originCity) {
        this.f0.f2 = originCity;
    }


    public String getOriginState() {
        return this.f0.f3;
    }

    public void setOriginState(String originState) {
        this.f0.f3 = originState;
    }


    public String getOriginCountry() {
        return this.f0.f4;
    }

    public void setOriginCountry(String originCountry) {
        this.f0.f4 = originCountry;
    }


    public String getOriginRegion() {
        return this.f0.f5;
    }

    public void setOriginRegion(String originRegion) {
        this.f0.f5 = originRegion;
    }


    public Double getOriginLatitude() {
        return this.f0.f6;
    }

    public void setOriginLatitude(Double originLatitude) {
        this.f0.f6 = originLatitude;
    }


    public Double getOriginLongitude() {
        return this.f0.f7;
    }

    public void setOriginLongitude(Double originLongitude) {
        this.f0.f7 = originLongitude;
    }


    public String getOriginICAO() {
        return this.f0.f8;
    }

    public void setOriginICAO(String icao) {
        this.f0.f8 = icao;
    }


    public Long getDepartureTimestamp() {
        return this.f1;
    }

    public void setDepartureTimestamp(Long departureTimestamp) {
        this.f1 = departureTimestamp;
    }


    public String getDestinationAirport() {
        return this.f2.f0;
    }

    public void setDestinationAirport(String destinationAirport) {
        this.f2.f0 = destinationAirport;
    }


    public String getDestinationTerminal() {
        return this.f2.f1;
    }

    public void setDestinationTerminal(String destinationTerminal) {
        this.f2.f1 = destinationTerminal;
    }


    public String getDestinationCity() {
        return this.f2.f2;
    }

    public void setDestinationCity(String destinationCity) {
        this.f2.f2 = destinationCity;
    }


    public String getDestinationState() {
        return this.f2.f3;
    }

    public void setDestinationState(String destinationState) {
        this.f2.f3 = destinationState;
    }


    public String getDestinationCountry() {
        return this.f2.f4;
    }

    public void setDestinationCountry(String destinationCountry) {
        this.f2.f4 = destinationCountry;
    }


    public String getDestinationRegion() {
        return this.f2.f5;
    }

    public void setDestinationRegion(String destinationRegion) {
        this.f2.f5 = destinationRegion;
    }


    public Double getDestinationLatitude() {
        return this.f2.f6;
    }

    public void setDestinationLatitude(Double destinationLatitude) {
        this.f2.f6 = destinationLatitude;
    }


    public Double getDestinationLongitude() {
        return this.f2.f7;
    }

    public void setDestinationLongitude(Double destinationLongitude) {
        this.f2.f7 = destinationLongitude;
    }


    public String getDestinationICAO() {
        return this.f2.f8;
    }

    public void setDestinationICAO(String icao) {
        this.f2.f8 = icao;
    }


    public Long getArrivalTimestamp() {
        return this.f3;
    }

    public void setArrivalTimestamp(Long arrivalTimestamp) {
        this.f3 = arrivalTimestamp;
    }


    public String getAirline() {
        return this.f4;
    }

    public void setAirline(String airline) {
        this.f4 = airline;
    }


    public Integer getFlightNumber() {
        return this.f5;
    }

    public void setFlightNumber(Integer flightNumber) {
        this.f5 = flightNumber;
    }


    public String getAircraftType() {
        return f6;
    }

    public void setAircraftType(String aircraftType) {
        this.f6 = aircraftType;
    }


    public Integer getMaxCapacity() {
        return this.f7;
    }

    public void setMaxCapacity(Integer maxCapacity) {
        this.f7 = maxCapacity;
    }


    public String getCodeShareInfo() {
        return this.f8;
    }

    public void setCodeShareInfo(String codeShareInfo) {
        this.f8 = codeShareInfo;
    }


    public Character getTrafficRestriction() {
        return this.f9;
    }

    public void setTrafficRestriction(Character trafficRestrictions) {
        this.f9 = trafficRestrictions;
    }


    public String getLastAirport() {
        return this.f10.f0;
    }

    public void setLastAirport(String lastAirport) {
        this.f10.f0 = lastAirport;
    }


    public String getLastTerminal() {
        return this.f10.f1;
    }

    public void setLastTerminal(String originTerminal) {
        this.f10.f1 = originTerminal;
    }


    public String getLastCity() {
        return this.f10.f2;
    }

    public void setLastCity(String originCity) {
        this.f10.f2 = originCity;
    }


    public String getLastState() {
        return this.f10.f3;
    }

    public void setLastState(String originState) {
        this.f10.f3 = originState;
    }


    public String getLastCountry() {
        return this.f10.f4;
    }

    public void setLastCountry(String originCountry) {
        this.f10.f4 = originCountry;
    }


    public String getLastRegion() {
        return this.f10.f5;
    }

    public void setLastRegion(String originRegion) {
        this.f10.f5 = originRegion;
    }


    public Double getLastLatitude() {
        return this.f10.f6;
    }

    public void setLastLatitude(Double originLatitude) {
        this.f10.f6 = originLatitude;
    }


    public Double getLastLongitude() {
        return this.f10.f7;
    }

    public void setLastLongitude(Double originLongitude) {
        this.f10.f7 = originLongitude;
    }


    public String getLastICAO() {
        return this.f10.f8;
    }

    public void setLastICAO(String icao) {
        this.f10.f8 = icao;
    }


    public Integer getLegCount() {
        return this.f11;
    }

    public void setLegCount(Integer newCount) {
        this.f11 = newCount;
    }


    public Integer getDepartureWindow() {
        return this.f12;
    }

    public void setDepartureWindow(Integer depWindow1) {
        this.f12 = depWindow1;
    }


    public Integer getFirstArrivalWindow() {
        return this.f13;
    }

    public void setFirstArrivalWindow(Integer arrWindow1) {
        this.f13 = arrWindow1;
    }


    public Integer getSecondArrivalWindow() {
        return this.f14;
    }

    public void setSecondArrivalWindow(Integer arrWindow2) {
        this.f14 = arrWindow2;
    }

}
