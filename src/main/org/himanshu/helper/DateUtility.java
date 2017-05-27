package org.himanshu.helper;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by himanshu on 5/27/2017.
 */
public class DateUtility implements Serializable {

    public static Boolean isDateValid(String line, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setLenient(false);

        try {
            dateFormat.parse(line);
            return true;
        } catch (ParseException e) {
            return false;
        }


    }
}
