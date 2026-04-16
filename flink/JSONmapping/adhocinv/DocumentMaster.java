package com.example.flink.jsonmapping.adhocinv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DocumentMaster {
    public String query = "INSERT INTO invoicetest.abc (`key`,`table`,nodeKeyRef,lastModifiedDate,email_id,`number`) VALUES ( ?, ?, ?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE `key` = VALUES(`key`)";

    private String flag;
    private String table;
    private String OP_TS;
    private String KAFKA_TS;
    private String TRAIL_FILE_POS;
    private String INVOICE_ID;
    private String TENANT_ID;
    private String SHIPMENT_NO;
    private String PARENT_EXC_SHIPMENT_NO;
    private String SUPPLY_TYPE_CODE;
    private String DOC_TYPE;
    private String DOC_VERSION;
    private String DOC_ISSUE_DATETIME;
    private String UPDATED_AT;
    private String TRX_STATUS_UPDATED_AT;
    private String UPDATED_BY;
    private String NODE_ID;
    private String STATUS;
    private String TRX_STATUS;
    private String TRX_CNT;
    private String ORGANIZATION_CODE;
    private String CGST_PRICE;
    private String SGST_PRICE;
    private String INVOICE_AMOUNT;

    @JsonProperty("INVOICE_DOC")
    private INVOICE_DOC invoiceDoc;

    public invoice_master() {
        super();
    }

    public String getKey() {
        if (invoiceDoc.getnode_id() == null) {
            return null;
        } else {
            return invoiceDoc.getnode_id();
        }
    }

    public INVOICE_DOC getInv() {
        return invoiceDoc;
    }

    public void setInv(INVOICE_DOC invoiceDoc) {
        this.invoiceDoc = invoiceDoc;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String gettable() {
        return table;
    }

    @JsonProperty("table")
    public void settable(String table) {
        this.table = table;
    }

    public String getOP_TS() throws Exception {
        return OP_TS;
    }

    @JsonProperty("op_ts")
    public void setOP_TS(String OP_TS) {
        this.OP_TS = OP_TS;
    }

    public String getKAFKA_TS() throws Exception {
        return KAFKA_TS;
    }

    @JsonProperty("current_ts")
    public void setKAFKA_TS(String KAFKA_TS) {
        this.KAFKA_TS = KAFKA_TS;
    }

    public String getTRAIL_FILE_POS() {
        return TRAIL_FILE_POS;
    }

    @JsonProperty("pos")
    public void getTRAIL_FILE_POS(String TRAIL_FILE_POS) {
        this.TRAIL_FILE_POS = TRAIL_FILE_POS;
    }

    public Timestamp getLAST_MODIFIED_DATE() {
        return new Timestamp(System.currentTimeMillis());
    }

    public String getINVOICE_ID() {
        return INVOICE_ID;
    }

    @JsonProperty("INVOICE_ID")
    public void setINVOICE_ID(String INVOICE_ID) {
        this.INVOICE_ID = INVOICE_ID;
    }

    public String getNODE_ID() {
        return NODE_ID;
    }

    @JsonProperty("NODE_ID")
    public void setNODE_ID(String NODE_ID) {
        this.NODE_ID = NODE_ID;
    }

    public String getITEM_ID() {
        return TENANT_ID;
    }

    @JsonProperty("TENANT_ID")
    public void setITEM_ID(String ITEM_ID) {
        this.TENANT_ID = ITEM_ID;
    }
//    public String getPRODUCT_CLASS() {
//        return PRODUCT_CLASS;
//    }
//    @JsonProperty("PRODUCT_CLASS")
//    public void setPRODUCT_CLASS(String PRODUCT_CLASS) {
//        this.PRODUCT_CLASS = PRODUCT_CLASS;
//    }
//    public String getMOQ() {
//        return MOQ;
//    }
//    @JsonProperty("MOQ")
//    public void setMOQ(String MOQ) {
//        this.MOQ = MOQ;
//    }
//    public String getCREATED_BY() {
//        return CREATED_BY;
//    }
//    @JsonProperty("CREATED_BY")
//    public void setCREATED_BY(String CREATED_BY) {
//        this.CREATED_BY = CREATED_BY;
//    }
//    public String getCREATED_TIME() {
//        return CREATED_TIME;
//    }
//    @JsonProperty("CREATED_TIME")
//    public void setCREATED_TIME(String CREATED_TIME) {
//        this.CREATED_TIME = CREATED_TIME;
//    }
//    public String getLAST_MODIFIED_BY() {
//        return LAST_MODIFIED_BY;
//    }
//    @JsonProperty("LAST_MODIFIED_BY")
//    public void setLAST_MODIFIED_BY(String LAST_MODIFIED_BY) {
//        this.LAST_MODIFIED_BY = LAST_MODIFIED_BY;
//    }
//    public String getLAST_MODIFIED_TIME() {
//        return LAST_MODIFIED_TIME;
//    }
//    @JsonProperty("LAST_MODIFIED_TIME")
//    public void setLAST_MODIFIED_TIME(String LAST_MODIFIED_TIME) {
//        this.LAST_MODIFIED_TIME = LAST_MODIFIED_TIME;
//    }

    public Timestamp getLastModifiedDate() {
        return new Timestamp(System.currentTimeMillis());
    }

    public Row getRow(Tuple6<String, String, String, Timestamp, String, String> value) throws Exception {
        Row row = Row.of(value.f0, value.f1, value.f2, Timestamp.valueOf(String.valueOf(value.f3)), value.f4, value.f5);
        return row;
    }

}
//private int i = 0;
//public Row getRowREdefined() throws Exception {
//    String key = this.getKey();
//    String table = this.gettable();
//    String nodeKeyRef = this.getInv().getnode_key_ref();
//    Timestamp lastModifiedDate = this.getLastModifiedDate();
//
//    List<ContactInfo> emails = this.getInv().getbuyer_dtls().getemails();
//    List<PhoneInfo> phones = this.getInv().getbuyer_dtls().getphones();
//
//    Row row = new Row(6); // Initialize Row with 6 fields to accommodate the 6th field (PhoneInfo number)
//    row.setField(0, key);
//    row.setField(1, table);
//    row.setField(2, nodeKeyRef);
//    row.setField(3, lastModifiedDate);
//
//    if (emails != null && i < emails.size()) {
//        row.setField(4, emails.get(i).getemail_id());
//    }
//
//    if (phones != null && i < phones.size()) {
//        row.setField(5, phones.get(i).getnumber());
//    }
//
//    return row;
//}

//    public List<Row> getRowREdefined() throws Exception {
//        List<Row> rows = new ArrayList<>();
//        String key = this.getKey();
//        String table = this.gettable();
//        String nodeKeyRef = this.getInv().getnode_key_ref();
//        Timestamp lastModifiedDate = this.getLastModifiedDate();
//
//        List<ContactInfo> emails = this.getInv().getbuyer_dtls().getemails();
//        List<PhoneInfo> phones = this.getInv().getbuyer_dtls().getphones();
//
//        int maxContactCount = Math.max(emails != null ? emails.size() : 0, phones != null ? phones.size() : 0);
//
//        for (int i = 0; i < maxContactCount; i++) {
//            Row row = new Row(6);
//            row.setField(0, key);
//            row.setField(1, table);
//            row.setField(2, nodeKeyRef);
//            row.setField(3, lastModifiedDate);
//
//            if (emails != null && i < emails.size()) {
//                row.setField(4, emails.get(i).getemail_id());
//            }
//
//            if (phones != null && i < phones.size()) {
//                row.setField(5, phones.get(i).getnumber());
//            }
//
//            rows.add(row);
//        }
//
//        return rows;
//    }

//    public List<Row> getRow() throws Exception {
//        List<Row> rows = new ArrayList<>();
//        String key = this.getKey();
//        String table = this.gettable();
//        String nodeKeyRef = this.getInv().getnode_key_ref();
//        Timestamp lastModifiedDate = this.getLastModifiedDate();
//
//        List<ContactInfo> emails = this.getInv().getbuyer_dtls().getemails();
//        List<PhoneInfo> phones = this.getInv().getbuyer_dtls().getphones();
//
//        int maxContactCount = Math.max(emails != null ? emails.size() : 0, phones != null ? phones.size() : 0);
//
//        for (int i = 0; i < maxContactCount; i++) {
//            Row row = new Row(9);
//            row.setField(0, key);
//            row.setField(1, table);
//            row.setField(2, nodeKeyRef);
//            row.setField(3, lastModifiedDate);
//
//            if (emails != null && i < emails.size()) {
//                row.setField(4, emails.get(i).getemail_id());
//            } else {
//                row.setField(4, null);
//            }
//
//            if (phones != null && i < phones.size()) {
//                row.setField(5, phones.get(i).getnumber());
//            } else {
//                row.setField(5, null);
//            }
//
//            rows.add(row);
//        }
//
//        return rows;
//    }




