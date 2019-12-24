package ua.org.zagoruiko.expenses.spark.etl.dto;

import java.io.Serializable;

public class CurrencyRateDTO implements Serializable {
    String r030;
    String txt;
    String cc;
    Float rate;
    String exchangedate;

    public CurrencyRateDTO() {

    }

    public CurrencyRateDTO(String r030, String txt, String cc, Float rate, String exchangedate) {
        this.r030 = r030;
        this.txt = txt;
        this.cc = cc;
        this.rate = rate;
        this.exchangedate = exchangedate;
    }

    public String getR030() {
        return r030;
    }

    public void setR030(String r030) {
        this.r030 = r030;
    }

    public String getTxt() {
        return txt;
    }

    public void setTxt(String txt) {
        this.txt = txt;
    }

    public String getCc() {
        return cc;
    }

    public void setCc(String cc) {
        this.cc = cc;
    }

    public Float getRate() {
        return rate;
    }

    public void setRate(Float rate) {
        this.rate = rate;
    }

    public String getExchangedate() {
        return exchangedate;
    }

    public void setExchangedate(String exchangedate) {
        this.exchangedate = exchangedate;
    }
}
