package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class MCTFilter implements CoGroupFunction<Tuple2<Flight, Flight>, MCTEntry, Tuple2<Flight, Flight>> {

    private ArrayList<MCTEntry> DDList;
    private ArrayList<MCTEntry> DIList;
    private ArrayList<MCTEntry> IDList;
    private ArrayList<MCTEntry> IIList;

    private ArrayList<MCTEntry> DDWithAPChangeList;
    private ArrayList<MCTEntry> DIWithAPChangeList;
    private ArrayList<MCTEntry> IDWithAPChangeList;
    private ArrayList<MCTEntry> IIWithAPChangeList;

    private long defaultDD = 20L;
    private long defaultDI = 60L;
    private long defaultID = 60L;
    private long defaultII = 60L;

    private long defaultDDWithAPChange = 240L;
    private long defaultDIWithAPChange = 240L;
    private long defaultIDWithAPChange = 240L;
    private long defaultIIWithAPChange = 240L;

    @Override
    public void coGroup(Iterable<Tuple2<Flight, Flight>> connections, Iterable<MCTEntry> mctEntries, Collector<Tuple2<Flight, Flight>> out) throws Exception {

        DDList = new ArrayList<MCTEntry>(1);
        DIList = new ArrayList<MCTEntry>(1);
        IDList = new ArrayList<MCTEntry>(1);
        IIList = new ArrayList<MCTEntry>(1);

        DDWithAPChangeList = new ArrayList<MCTEntry>(1);
        DIWithAPChangeList = new ArrayList<MCTEntry>(1);
        IDWithAPChangeList = new ArrayList<MCTEntry>(1);
        IIWithAPChangeList = new ArrayList<MCTEntry>(1);

        String stat = null;
        String dep = null;
        String arr = null;

        for (MCTEntry entry : mctEntries) {
            stat = entry.getStat();
            arr = entry.getArrival();
            dep = entry.getDeparture();
            if (dep.isEmpty()) {
                if (stat.equals("DD")) {
                    DDList.add(entry);
                } else if (stat.equals("DI")) {
                    DIList.add(entry);
                } else if (stat.equals("ID")) {
                    IDList.add(entry);
                } else if (stat.equals("II")) {
                    IIList.add(entry);
                } else {
                    throw new Exception("Unknown stat: " + entry.toString());
                }
            } else if (!arr.equals(dep)) {
                if (stat.equals("DD")) {
                    DDWithAPChangeList.add(entry);
                } else if (stat.equals("DI")) {
                    DIWithAPChangeList.add(entry);
                } else if (stat.equals("ID")) {
                    IDWithAPChangeList.add(entry);
                } else if (stat.equals("II")) {
                    IIWithAPChangeList.add(entry);
                } else {
                    throw new Exception("Unknown stat: " + entry.toString());
                }
            }
        }

        Collections.sort(DDList, new ScoreComparator());
        Collections.sort(DIList, new ScoreComparator());
        Collections.sort(IDList, new ScoreComparator());
        Collections.sort(IIList, new ScoreComparator());
        Collections.sort(DDWithAPChangeList, new ScoreComparator());
        Collections.sort(DIWithAPChangeList, new ScoreComparator());
        Collections.sort(IDWithAPChangeList, new ScoreComparator());
        Collections.sort(IIWithAPChangeList, new ScoreComparator());

        ArrayList<MCTEntry> connStatList = null;
        long defaultMCT = 0L;
        for (Tuple2<Flight, Flight> conn : connections) {
            if (conn.f0.getDestinationAirport().equals(conn.f1.getOriginAirport())) {
                if (conn.f0.getLastCountry().equals(conn.f0.getDestinationCountry())) {
                    if (conn.f1.getOriginCountry().equals(conn.f1.getDestinationCountry())) {
                        connStatList = DDList;
                        defaultMCT = defaultDD;
                    } else {
                        connStatList = DIList;
                        defaultMCT = defaultDI;
                    }
                } else {
                    if (conn.f1.getLastCountry().equals(conn.f1.getDestinationCountry())) {
                        connStatList = IDList;
                        defaultMCT = defaultID;
                    } else {
                        connStatList = IIList;
                        defaultMCT = defaultII;
                    }
                }
            } else {
                if (conn.f0.getLastCountry().equals(conn.f0.getDestinationCountry())) {
                    if (conn.f1.getOriginCountry().equals(conn.f1.getDestinationCountry())) {
                        connStatList = DDWithAPChangeList;
                        defaultMCT = defaultDDWithAPChange;
                    } else {
                        connStatList = DIWithAPChangeList;
                        defaultMCT = defaultDIWithAPChange;
                    }
                } else {
                    if (conn.f1.getLastCountry().equals(conn.f1.getDestinationCountry())) {
                        connStatList = IDWithAPChangeList;
                        defaultMCT = defaultIDWithAPChange;
                    } else {
                        connStatList = IIWithAPChangeList;
                        defaultMCT = defaultIIWithAPChange;
                    }
                }
            }
            long MCT = 0L;
            int bestResult = 0;
            // search for best rule in connStatList
            MCTEntry bestRule = null;
            for (MCTEntry e : connStatList) {
                if (match(conn, e)) {
                    bestRule = e;
                    break; // first hit is best rule since the list is sorted by score
                }
            }
            if (bestRule != null) {
                MCT = bestRule.getMCT();
            } else {
                MCT = defaultMCT;
            }
            if (checkWithMCT(conn, MCT)) {
                out.collect(conn);
            }
        }
    }

    private static boolean checkWithMCT(Tuple2<Flight, Flight> conn, long MCT) {
        if (MCT >= 999L) {
            return false;
        }
        long MinCT = MCT * 60L * 1000L;
        long hubTime = conn.f1.getDepartureTimestamp() - conn.f0.getArrivalTimestamp();
        if (hubTime < MinCT) {
            return false;
        } else {
            return true;
        }
    }

    public static class ScoreComparator implements Comparator<MCTEntry> {

        @Override
        public int compare(MCTEntry mctEntry, MCTEntry mctEntry2) {
            // descending order
            return scoreRule(mctEntry2) - scoreRule(mctEntry);
        }

        @Override
        public boolean equals(Object o) {
            // MCT rules are assumed to be unique
            return (this == o);
        }
    }

    public static int scoreRule(MCTEntry rule) {
        int result = 0;
        if (rule.getStat().isEmpty())
            return 0;
        if (!rule.getDeparture().isEmpty())
            result += 1 << 0;
        if (!rule.getArrival().isEmpty())
            result += 1 << 1;
        if (!rule.getDepTerminal().isEmpty())
            result += 1 << 2;
        if (!rule.getArrTerminal().isEmpty())
            result += 1 << 3;
        if (!rule.getNextRegion().isEmpty())
            result += 1 << 4;
        if (!rule.getPrevRegion().isEmpty())
            result += 1 << 5;
        if (!rule.getNextCountry().isEmpty())
            result += 1 << 6;
        if (!rule.getPrevCountry().isEmpty())
            result += 1 << 7;
        if (!rule.getNextState().isEmpty())
            result += 1 << 8;
        if (!rule.getPrevState().isEmpty())
            result += 1 << 9;
        if (!rule.getNextCity().isEmpty())
            result += 1 << 10;
        if (!rule.getPrevCity().isEmpty())
            result += 1 << 11;
        if (!rule.getNextAirport().isEmpty())
            result += 1 << 12;
        if (!rule.getPrevAirport().isEmpty())
            result += 1 << 13;
        if (!rule.getDepAircraft().isEmpty())
            result += 1 << 14;
        if (!rule.getArrAircraft().isEmpty())
            result += 1 << 15;
        if (!rule.getDepCarrier().isEmpty()) {
            result += 1 << 16;
            if (rule.getOutFlightNumber().intValue() != 0) {
                result += 1 << 18;
            }
        }
        if (!rule.getArrCarrier().isEmpty()) {
            result += 1 << 17;
            if (rule.getInFlightNumber().intValue() != 0) {
                result += 1 << 19;
            }
        }
        return result;
    }

    private static boolean match(Tuple2<Flight, Flight> conn, MCTEntry rule) {
        if(conn.f0.getArrivalTimestamp() < rule.getValidFrom() || conn.f1.getDepartureTimestamp() > rule.getValidUntil()) {
            return false;
        }
        if (!rule.getArrival().isEmpty()) {
            if (!conn.f0.getDestinationAirport().equals(rule.getArrival())) {
                return false;
            }
        }
        if (!rule.getDeparture().isEmpty()) {
            if (!conn.f1.getLastAirport().equals(rule.getDeparture())) {
                return false;
            }
        }
        if (!rule.getArrCarrier().isEmpty()) {
            if (!conn.f0.getAirline().equals(rule.getArrCarrier())) {
                return false;
            } else {
                if (rule.getInFlightNumber().intValue() != 0) {
                    if (!(conn.f0.getFlightNumber() >= rule.getInFlightNumber() &&
                            conn.f0.getFlightNumber() <= rule.getInFlightEOR())) {
                        return false;
                    }
                }
            }
        }
        if (!rule.getDepCarrier().isEmpty()) {
            if (!conn.f1.getAirline().equals(rule.getDepCarrier())) {
                return false;
            } else {
                if (rule.getOutFlightNumber().intValue() != 0) {
                    if (!(conn.f1.getFlightNumber() >= rule.getOutFlightNumber() &&
                            conn.f1.getFlightNumber() <= rule.getOutFlightEOR())) {
                        return false;
                    }
                }
            }
        }
        if (!rule.getArrAircraft().isEmpty()) {
            if (!conn.f0.getAircraftType().equals(rule.getArrAircraft())) {
                return false;
            }
        }
        if (!rule.getDepAircraft().isEmpty()) {
            if (!conn.f1.getAircraftType().equals(rule.getDepAircraft())) {
                return false;
            }
        }
        if (!rule.getArrTerminal().isEmpty()) {
            if (!conn.f0.getDestinationTerminal().equals(rule.getArrTerminal())) {
                return false;
            }
        }
        if (!rule.getDepTerminal().isEmpty()) {
            if (!conn.f1.getLastTerminal().equals(rule.getDepTerminal())) {
                return false;
            }
        }
        if (!rule.getPrevCountry().isEmpty()) {
            if (!conn.f0.getLastCountry().equals(rule.getPrevCountry())) {
                return false;
            }
        }
        if (!rule.getPrevCity().isEmpty()) {
            if (!conn.f0.getLastCity().equals(rule.getPrevCity())) {
                return false;
            }
        }
        if (!rule.getPrevAirport().isEmpty()) {
            if (!conn.f0.getLastAirport().equals(rule.getPrevAirport())) {
                return false;
            }
        }
        if (!rule.getNextCountry().isEmpty()) {
            if (!conn.f1.getDestinationCountry().equals(rule.getNextCountry())) {
                return false;
            }
        }
        if (!rule.getNextCity().isEmpty()) {
            if (!conn.f1.getDestinationCity().equals(rule.getNextCity())) {
                return false;
            }
        }
        if (!rule.getNextAirport().isEmpty()) {
            if (!conn.f1.getDestinationAirport().equals(rule.getNextAirport())) {
                return false;
            }
        }
        if (!rule.getPrevState().isEmpty()) {
            if (!conn.f0.getLastState().equals(rule.getPrevState())) {
                return false;
            }
        }
        if (!rule.getNextState().isEmpty()) {
            if (!conn.f1.getDestinationState().equals(rule.getNextState())) {
                return false;
            }
        }
        if (!rule.getPrevRegion().isEmpty()) {
            // SCH is a real subset of EUR
            if (!rule.getPrevRegion().equals("EUR") || !rule.getPrevRegion().equals("SCH")) {
                if (!conn.f0.getLastRegion().equals(rule.getPrevRegion())) {
                    return false;
                }
            } else {
                if (!conn.f0.getLastRegion().equals("SCH") || !conn.f0.getLastRegion().equals("EUR")) {
                    return false;
                }
            }
        }
        if (!rule.getNextRegion().isEmpty()) {
            // SCH is a real subset of EUR
            if (!rule.getNextRegion().equals("EUR") || !rule.getNextRegion().equals("SCH")) {
                if (!conn.f1.getDestinationRegion().equals(rule.getNextRegion())) {
                    return false;
                }
            } else {
                if (!conn.f1.getDestinationRegion().equals("SCH") || !conn.f1.getDestinationRegion().equals("EUR")) {
                    return false;
                }
            }
        }
        return true;
    }
}