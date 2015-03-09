package de.tuberlin.dima.old.join;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class RuleSortTest extends TestCase {

    private final static long SEED = 1337L;
    private final static int MAX = 1000;
    private final static Random rnd  = new Random(SEED);
    private final static String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    @Test
    public void testSort() {
        ArrayList<MCTEntry> list = generateInput();
        Collections.sort(list, new FlightConnectionJoiner.MCTFilter.ScoreComparator());
        int last = Integer.MAX_VALUE;
        int score = 0;
        for (MCTEntry e : list) {
            score = FlightConnectionJoiner.MCTFilter.scoreRule(e);
            System.out.println(FlightConnectionJoiner.MCTFilter.scoreRule(e) + ": " + e.toString());
            if(score > last) {
                fail("descending order required!");
            }
            last = FlightConnectionJoiner.MCTFilter.scoreRule(e);
        }
    }

    private ArrayList<MCTEntry> generateInput() {
        ArrayList<MCTEntry> list = new ArrayList<MCTEntry>();
        for( int i = 0; i < MAX; i++) {
            list.add(generateEntry());
        }
        return list;
    }

    private MCTEntry generateEntry() {
        MCTEntry entry = new MCTEntry();
        entry.f0 = randomString(3);
        entry.f1 = randomString(2);
        if(rnd.nextBoolean()) {
            entry.f2 = randomString(3);
        } else {
            entry.f2 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f3 = randomString(2);
        } else {
            entry.f3 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f4 = randomString(2);
        } else {
            entry.f4 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f5 = randomString(3);
        } else {
            entry.f5 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f6 = randomString(3);
        } else {
            entry.f6 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f7 = randomString(2);
        } else {
            entry.f7 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f8 = randomString(2);
        } else {
            entry.f8 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f9 = randomString(2);
        } else {
            entry.f9 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f10 = randomString(3);
        } else {
            entry.f10 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f11 = randomString(3);
        } else {
            entry.f11 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f12 = randomString(2);
        } else {
            entry.f12 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f13 = randomString(3);
        } else {
            entry.f13 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f14 = randomString(3);
        } else {
            entry.f14 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f15 = rnd.nextInt(10000);
        } else {
            entry.f15 = 0;
        }
        if(rnd.nextBoolean()) {
            entry.f16 = rnd.nextInt(10000);
        } else {
            entry.f16 = 0;
        }
        if(rnd.nextBoolean()) {
            entry.f17 = rnd.nextInt(10000);
        } else {
            entry.f17 = 0;
        }
        if(rnd.nextBoolean()) {
            entry.f18 = rnd.nextInt(10000);
        } else {
            entry.f18 = 0;
        }
        if(rnd.nextBoolean()) {
            entry.f19 = randomString(2);
        } else {
            entry.f19 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f20 = randomString(2);
        } else {
            entry.f20 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f21 = randomString(3);
        } else {
            entry.f21 = "";
        }
        if(rnd.nextBoolean()) {
            entry.f22 = randomString(3);
        } else {
            entry.f22 = "";
        }
        entry.f23 = rnd.nextInt(1000);
        return entry;
    }

    private String randomString(int len) {
        if(len <= 0) {
            return null;
        }
        char[] text = new char[len];
        for (int i = 0; i < len; i++)
        {
            text[i] = alphabet.charAt(rnd.nextInt(alphabet.length()));
        }
        return new String(text);
    }
}
