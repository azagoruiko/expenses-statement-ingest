package ua.org.zagoruiko.expenses.spark.etl.routines;


import org.junit.Assert;
import org.junit.Test;


public class AlfaBankTest {

    @Test
    public void cleanAmount() {
        String result = AlfaBank.cleanAmount("- 6 021,81 - 226,81 EUR");
        Assert.assertEquals("-6021.81", result);

        result = AlfaBank.cleanAmount("- 36 741,03");
        Assert.assertEquals("-36741.03", result);
    }

}