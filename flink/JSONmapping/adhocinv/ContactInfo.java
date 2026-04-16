package com.example.flink.jsonmapping.adhocinv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ContactInfo {
    private String type;
    private String email_id;


    public String getemail_id() {
        return email_id;
    }
    @JsonProperty("email_id")

    public void setemail_id(String email_id) {
        this.email_id = email_id;
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


