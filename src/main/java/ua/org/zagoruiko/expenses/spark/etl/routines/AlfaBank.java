package ua.org.zagoruiko.expenses.spark.etl.routines;

import java.io.Serializable;

public class AlfaBank implements Serializable {
    public static String cleanAmount(String amount) {
        if (amount == null) {
            return "0";
        }

        StringBuilder sb = new StringBuilder();
        boolean decimalFound = false;
        String numberChars = "-0123456789";
        for (Character ch : amount.toCharArray()) {
            if (numberChars.contains(ch.toString())) {
                sb.append(ch);
            }
            else if (ch == ' ' && decimalFound) {
                break;
            }
            else if (ch == ',') {
                sb.append('.');
                decimalFound = true;
            }
        }
        return sb.toString();
    }
}
