package com.example.flink.jsonmapping.adhocinv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Document {

    private String node_id;

    private String node_key_ref;

    private Participant buyer_dtls;




    public String getnode_id() {
        return node_id;
    }
    @JsonProperty("node_id")
    public void setnode_id(String node_id) {
        this.node_id = node_id;
    }

    public String getnode_key_ref() {
        return node_key_ref;
    }
    @JsonProperty("node_key_ref")
    public void setnode_key_ref(String node_key_ref) {
        this.node_key_ref = node_key_ref;
    }

    public Participant getbuyer_dtls() {
        return buyer_dtls;
    }
    @JsonProperty("buyer_dtls")
    public void setbuyer_dtls(Participant buyer_dtls) {
        this.buyer_dtls = buyer_dtls;
    }

    public String isNull(String columnName, Integer startPos, Integer endPos) {
        if (columnName == null) {
            return null;
        } else {
            return columnName.substring(startPos, endPos);
        }
    }
}


