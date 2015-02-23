package com.amadeus.pcb.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class ParseTest {

    public static void main(String[] args) {
        String date = "2014-10-25";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date day = new Date();
        try {
            day = format.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(day.getTime());
    }
}
