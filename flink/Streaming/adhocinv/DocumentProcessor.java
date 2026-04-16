package com.example.flink.streaming.adhocinv;


import com.example.flink.jsonmapping.adhocinv.ContactInfo;
import com.example.flink.jsonmapping.adhocinv.PhoneInfo;
import com.example.flink.jsonmapping.adhocinv.DocumentMaster;
import com.example.flink.jsonmapping.CustomKafkaDeserializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class DocumentProcessor {
    public static String hana, user, pass, kafka, source = "";

    public static void execute() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        String filename = "config.properties";
        hana = getRequiredEnvVar("HANA_URL", "hana");
        user = getRequiredEnvVar("HANA_USER", "user_hana");
        pass = getRequiredEnvVar("HANA_PASSWORD", "password_hana");
        kafka = getRequiredEnvVar("KAFKA_BROKERS", "kafkaacl");
        // Note: Credentials now come from environment variables for security

        String HanadriverName = "com.sap.db.jdbc.Driver";
        String HanaURL = hana;
        String HanaPassword = pass;
        String HanaUser = user;
        String groupId = "flink_yohoho";

        String topic1 = "FLINK_TEST";


        //long checkpointInterval = 300000;
        long checkpointInterval = 1200000;
        CheckpointingMode checkpointMode = CheckpointingMode.EXACTLY_ONCE;
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(150000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(600000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        String dbUrl = getRequiredEnvVar("DB_URL", "jdbc:mysql://localhost:3306?allowPublicKeyRetrieval=true&useSSL=false");
        String dbUser = getRequiredEnvVar("DB_USER", null);
        String dbPassword = getRequiredEnvVar("DB_PASSWORD", null);

        // Configure JDBC output format with environment variables for secure credential passing
        JDBCOutputFormat.JDBCOutputFormatBuilder outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl(dbUrl)
                .setUsername(dbUser)
                .setPassword(dbPassword)
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR});


        Properties properties = new Properties();
                // Kafka ACL configuration with SCRAM authentication is handled separately via secure configuration
        // DO NOT hardcode credentials directly - use external secure credential management systems
        // Kafka Kerberos configuration is handled via environment variables and not exposed here
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("flink.partition-discovery.interval-millis", "6000");

        Pattern topicPattern = Pattern.compile("(^(^" + topic1 + "))");

        // Process Kafka topic using custom deserialization
        DataStream<Tuple2<String, String>> readStreamKafka = env.addSource(new FlinkKafkaConsumer<>(topicPattern, new CustomKafkaDeserializationSchema(), properties)).filter(row -> row.f1.contains("\"op_type\":\"I\"")).setParallelism(2);
        env.enableCheckpointing(checkpointInterval, checkpointMode);

        // Filter and process ORDERS events
        SingleOutputStreamOperator<String> readStreamtest = readStreamKafka.filter(row -> row.f0.equals(topic1)).process(
                new ProcessFunction<Tuple2<String, String>, String>() {
                    @Override
                    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) {
                        out.collect(value.f1.replace("\"{\\", "{").replace("\\", "").replace("}\"}", "}}"));
                    }
                });


        readStreamtest.print();

        /**
         * TESTESTESTESTESTESTESTSTEST
         */


        // Parse the JSON and convert it to invoice_master objects
        DataStream<invoice_master> invoiceDataStream = readStreamtest.map(new JsonToinvoice_masterMapper());

        // Explode the rows for each ContactInfo and PhoneInfo
        DataStream<Tuple6<String, String, String, Timestamp, String, String>> resultDataStream = invoiceDataStream.flatMap(new ExplodeFunction());

//        resultDataStream.print();
        String sql = "INSERT INTO invoicetest.abc (`key`,`table`,nodeKeyRef,lastModifiedDate,email_id,`number`) VALUES ( ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE `key` = VALUES(`key`)";

        SINK_TEST(resultDataStream, outputFormat);

        env.execute("invtest");
    }
    public static class JsonToinvoice_masterMapper implements MapFunction<String, invoice_master> {
        @Override
        public invoice_master map(String json) throws Exception {
            // Use your favorite JSON library to deserialize the JSON string into invoice_master object
            // For example, you can use Jackson ObjectMapper:
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(json, invoice_master.class);
        }
    }

    // Custom flat map function to explode rows for each ContactInfo and PhoneInfo
    public static class ExplodeFunction implements FlatMapFunction<invoice_master, Tuple6<String, String, String, Timestamp, String, String>> {
        @Override
        public void flatMap(invoice_master invoice, Collector<Tuple6<String, String, String, Timestamp, String, String>> out) {
            List<ContactInfo> emails = DocumentProcessor.getInv().getbuyer_dtls().getemails();
            List<PhoneInfo> phones = DocumentProcessor.getInv().getbuyer_dtls().getphones();

            // Combine emails and phones based on index
            for (int i = 0; i < Math.max(emails.size(), phones.size()); i++) {
                String emailId = i < emails.size() ? emails.get(i).getemail_id() : null;
                String phoneNumber = i < phones.size() ? phones.get(i).getnumber() : null;
                out.collect(new Tuple6<>(DocumentProcessor.getKey(), DocumentProcessor.gettable(), DocumentProcessor.getInv().getnode_key_ref(), DocumentProcessor.getLastModifiedDate(), emailId, phoneNumber));
            }
        }
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

    private static void SINK_TEST(DataStream<Tuple6<String, String, String, Timestamp, String, String>> readStream, JDBCOutputFormat.JDBCOutputFormatBuilder jdbcHanaIntput) {
        invoice_master objTable = new invoice_master();
        SingleOutputStreamOperator<Row> jdbcStream = readStream.map(new MapFunction<Tuple6<String, String, String, Timestamp, String, String>, Row>() {
            @Override
            public Row map(Tuple6<String, String, String, Timestamp, String, String> value) throws Exception {
                Row row = Row.of(value.f0, value.f1, value.f2, Timestamp.valueOf(String.valueOf(value.f3)), value.f4, value.f5);
                return row;
            }
        });
        jdbcStream.writeUsingOutputFormat(jdbcHanaIntput.setQuery(objTable.query).setBatchInterval(1).finish()).name("SINK_TEST").setParallelism(1);
    }
}







