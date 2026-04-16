package com.example.flink.jsonmapping.kerbkafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

@JsonIgnoreProperties(ignoreUnknown = true)

public class InventoryRecord {
    public String HanaQuery = "UPSERT HADMIN.InventoryRecord (TENANT_ID,STPNAAM,CODE_TYPE,ITEM_CODE,NEWAVBLBAL,BATCH_ID,CLIENT_NAME,REQUEST_TIME,TYPES,REQUESTID,LAST_MODIFIED_DATE) VALUES (?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY";
    //public String HanaQuery = "UPSERT RR_ANALYST.InventoryRecord_TEST_PROD (TENANT_ID,STPNAAM,CODE_TYPE,ITEM_CODE,NEWAVBLBAL,BATCH_ID,CLIENT_NAME,REQUEST_TIME,TYPES,REQUESTID,LAST_MODIFIED_DATE) VALUES (?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY";

    //public String HanaQuery = "UPSERT HADMIN.InventoryRecord (TENANT_ID,STPNAAM,CODE_TYPE,ITEM_CODE,NEWAVBLBAL,BATCH_ID,CLIENT_NAME,REQUEST_TIME,TYPES,REQUESTID,LAST_MODIFIED_DATE) VALUES (?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY";
    public String HanaDelQuery =  "DELETE FROM HADMIN.InventoryRecord where (1=1 or (TENANT_ID=? AND STPNAAM=? AND ITEM_CODE=?)) AND OP_TYPE='D'";


    private String TENANT_ID;
    private String STPNAAM;
    private String CODE_TYPE;
    private String ITEM_CODE;
    private String NEWAVBLBAL;
    private String BATCH_ID;
    private String CLIENT_NAME;
    private String REQUEST_TIME;
    private String TYPES;
    private String REQUESTID;

    public InventoryRecord() {super();}

    public String getKey() {
        if(TENANT_ID == null || STPNAAM == null || ITEM_CODE == null)
        {
            return null;
        }
        else {
            return TENANT_ID+STPNAAM+ITEM_CODE;
        }
    }

    public Timestamp getLAST_MODIFIED_DATE() {
        return new Timestamp(System.currentTimeMillis());
    }

    public String getTENANT_ID() {
        return TENANT_ID;
    }
    @JsonProperty("tenantId")
    public void setTENANT_ID(String TENANT_ID) {
        this.TENANT_ID = TENANT_ID;
    }

    public String getSTPNAAM() {
        return STPNAAM;
    }
    @JsonProperty("stpNaam")
    public void setSTPNAAM(String STPNAAM) {
        this.STPNAAM = STPNAAM;
    }

    public String getCODE_TYPE() {
        return CODE_TYPE;
    }
    @JsonProperty("codeType")
    public void setCODE_TYPE(String CODE_TYPE) {
        this.CODE_TYPE = CODE_TYPE;
    }

    public String getITEM_CODE() {
        return ITEM_CODE;
    }
    @JsonProperty("itemCode")
    public void setITEM_CODE(String ITEM_CODE) {
        this.ITEM_CODE = ITEM_CODE;
    }

    public String getNEWAVBLBAL() {
        return NEWAVBLBAL;
    }
    @JsonProperty("newAvblBal")
    public void setNEWAVBLBAL(String NEWAVBLBAL) {
        this.NEWAVBLBAL = NEWAVBLBAL;
    }

    public String getBATCH_ID() {
        return BATCH_ID;
    }
    @JsonProperty("batchId")
    public void setBATCH_ID(String BATCH_ID) {
        this.BATCH_ID = BATCH_ID;
    }

    public String getCLIENT_NAME() {
        return CLIENT_NAME;
    }
    @JsonProperty("clientName")
    public void setCLIENT_NAME(String CLIENT_NAME) {
        this.CLIENT_NAME = CLIENT_NAME;
    }

    public String getREQUEST_TIME() {
        return REQUEST_TIME;
    }
    @JsonProperty("requestTime")
    public void setREQUEST_TIME(String REQUEST_TIME) {
        this.REQUEST_TIME = REQUEST_TIME;
    }

    public String getTYPES() {
        return TYPES;
    }
    @JsonProperty("type")
    public void setTYPES(String TYPES) {
        this.TYPES = TYPES;
    }

    public String getREQUESTID() {
        return REQUESTID;
    }
    @JsonProperty("requestId")
    public void setREQUESTID(String REQUESTID) {
        this.REQUESTID = REQUESTID;
    }


    public Row getRow () throws Exception
    {
        Row row = new Row(11);
        row.setField(0, this.getTENANT_ID());
        row.setField(1, this.getSTPNAAM());
        row.setField(2, this.getCODE_TYPE());
        row.setField(3, this.getITEM_CODE());
        row.setField(4, this.getNEWAVBLBAL());
        row.setField(5, this.getBATCH_ID());
        row.setField(6, this.getCLIENT_NAME());
        row.setField(7, this.getREQUEST_TIME());
        row.setField(8, this.getTYPES());
        row.setField(9, this.getREQUESTID());
        row.setField(10, this.getLAST_MODIFIED_DATE());
        return row;
    }

    public Row getDeleteRow() throws Exception {
        Row row = new Row(4);
        row.setField(0, this.getTENANT_ID());
        row.setField(1, this.getSTPNAAM());
        row.setField(3, this.getITEM_CODE());
        return row;
    }
}


