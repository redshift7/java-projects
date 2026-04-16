package com.example.flink.jsonmapping;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class ProducerStringSerializationSchema implements KafkaSerializationSchema<Tuple2<String, String>> {

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> stringStringTuple2, @Nullable Long aLong) {
        return new ProducerRecord<byte[], byte[]>("kafka_topic_orders", stringStringTuple2.f1.getBytes(StandardCharsets.UTF_8));
    }
}






