package com.example.flink.jsonmapping;

import com.example.flink.jsonmapping.adhocinv.DocumentMaster;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.types.Row;


public class InvoiceMasterFlatMapFunction implements FlatMapFunction<DocumentMaster, Row> {

    @Override
    public void flatMap(DocumentMaster invoice, Collector<Row> collector) throws Exception {
        Row row = invoice.getRow();
        collector.collect(row);
    }
}


