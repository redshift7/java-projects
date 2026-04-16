package com.example.flink.jsonmapping.adhocinv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PhoneInfo {
    private String type;
    private String country_code;
    private String number;

    public String getcountry_code() {
        return country_code;
    }
    @JsonProperty("country_code")

    public void setcountry_code(String country_code) {
        this.country_code = country_code;
    }

    public String getnumber() {
        return number;
    }
    @JsonProperty("number")

    public void setnumber(String number) {
        this.number = number;
    }

    public String gettype() {
        return type;
    }
    @JsonProperty("type")

    public void settype(String type) {
        this.type = type;
    }

    public String isNull(String columnName, Integer startPos, Integer endPos) {
        if (columnName == null) {
            return null;
        } else {
            return columnName.substring(startPos, endPos);
        }
    }
}


