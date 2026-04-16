
package com.example.flink.jsonmapping.adhocinv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Participant {
    private List<ContactInfo> emails;
    private List<PhoneInfo> phones;

    public List<ContactInfo> getemails() {
        return emails;
    }
    @JsonProperty("emails")

    public void setemails(List<ContactInfo> emails) {
        this.emails = emails;
    }

    public List<PhoneInfo> getphones() {
        return phones;
    }
    @JsonProperty("phones")

    public void setphones(List<PhoneInfo> phones) {
        this.phones = phones;
    }


    public String isNull(String columnName, Integer startPos, Integer endPos) {
        if (columnName == null) {
            return null;
        } else {
            return columnName.substring(startPos, endPos);
        }
    }
}


