package com.example.flink.streaming.kerbkafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.flink.jsonmapping.CustomKafkaDeserializationSchema;
import com.example.flink.jsonmapping.kerbkafka.InventoryRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;


public class AuditGroupProcessor {
    public static String hana,user,pass,kafka,source="";
    public static void execute() throws Exception

    {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Kerberos configuration is handled via system properties and environment variables
        // See deployment documentation for required setup of krb5.conf and keytab files

        Properties prop = new Properties();
        String filename = "config.properties";
        InputStream input = AuditGroupProcessor.class.getClassLoader().getResourceAsStream(filename);


        hana = getRequiredEnvVar("HANA_URL", "hana");
        user = getRequiredEnvVar("HANA_USER", "user_hana");
        pass = getRequiredEnvVar("HANA_PASSWORD", "password_hana");
        kafka = getRequiredEnvVar("KAFKA_BROKERS", "kafka_kerbprod");
        // Note: Credentials loaded from environment variables for security

        String HanadriverName = "com.sap.db.jdbc.Driver";
        String HanaURL = hana;
        String HanaPassword = pass;
        String HanaUser = user;
        String groupId = "flink_test";

        String topic1 = "kafka_topic_audit_1";
        String topic2 = "kafka_topic_audit_2";
        String topic3 = "kafka_topic_audit_3";
        String topic4 = "kafka_topic_audit_4";


        long checkpointInterval = 300000;
//        long checkpointInterval = 1200000;
        CheckpointingMode checkpointMode = CheckpointingMode.EXACTLY_ONCE;
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(150000);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(600000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //Define Jdbc Output Format Builder
        JDBCOutputFormat.JDBCOutputFormatBuilder jdbcHanaIntput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(HanadriverName)
                .setDBUrl(HanaURL)
                .setUsername(HanaUser)
                .setPassword(HanaPassword);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("flink.partition-discovery.interval-millis", "6000");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getRequiredEnvVar("SSL_TRUSTSTORE_PATH", null));
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "GSSAPI");
        properties.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        
        // Kerberos SASL configuration with environment variables
        String kerberosPrincipal = getRequiredEnvVar("KERBEROS_PRINCIPAL", null);
        String kerberosKeytab = getRequiredEnvVar("KERBEROS_KEYTAB_PATH", null);
        
        properties.setProperty("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required "
                + "useKeyTab=true "
                + "keyTab=\"" + kerberosKeytab + "\" "
                + "principal=\"" + kerberosPrincipal + "\" "
                + "serviceName=\"kafka\";")



        Pattern topicPattern = Pattern.compile("(^(^"+topic1+"|^"+topic2+"|^"+topic3+"|^"+topic4+"))");

        DataStream<Tuple2<String, String>> readStreamKafka = env.addSource(new FlinkKafkaConsumer<>(topicPattern, new CustomKafkaDeserializationSchema(),properties)).setParallelism(4);
        env.enableCheckpointing(checkpointInterval, checkpointMode);


        //InventoryRecord
        SingleOutputStreamOperator<String> readStreamInventoryRecord = readStreamKafka.process(
                new ProcessFunction<Tuple2<String,String>, String>() {
                    @Override
                    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) {
                        out.collect(value.f1);
                    }
                });


        SINK_InventoryRecord(readStreamInventoryRecord,jdbcHanaIntput);
        env.execute("AuditGroupProcessor");
        //env.execute();
    }

    private static void SINK_InventoryRecord(SingleOutputStreamOperator<String> readStream, JDBCOutputFormat.JDBCOutputFormatBuilder jdbcHanaIntput){
        ObjectMapper OM = new ObjectMapper();
        InventoryRecord objTable = new InventoryRecord();
        SingleOutputStreamOperator<InventoryRecord> filteredStream = readStream.map(new MapFunction<String, InventoryRecord>() {
            @Override
            public InventoryRecord map(String input) throws IOException {
                return OM.readValue(input, InventoryRecord.class);
            }
        }).filter(row -> row.getKey()!=null);

        SingleOutputStreamOperator<Row> jdbcStream = filteredStream.map(new MapFunction<InventoryRecord, Row>() {
            @Override
            public Row map(InventoryRecord InventoryRecord) throws Exception {
                return InventoryRecord.getRow();
            }
        });
        //upsert
        jdbcStream.writeUsingOutputFormat(jdbcHanaIntput.setQuery(objTable.HanaQuery).setBatchInterval(1).finish()).name("KafkaHanaUpsertJdbcSink(SINK_InventoryRecord)").setParallelism(1);
    }

    private static String getRequiredEnvVar(String varName, String defaultValue) {
        String value = System.getenv(varName);
        if (value == null || value.isEmpty()) {
            if (defaultValue == null) {
                throw new IllegalStateException("Required environment variable not set: " + varName);
            }
            return defaultValue;
        }
        return value;
    }

}


