# Java Projects - Apache Flink Streaming

Real-time stream processing application for handling invoice data using Apache Flink and Kafka.

## Overview

This repository contains Flink streaming jobs that:
- Consume invoice events from Kafka
- Parse and validate JSON invoice data
- Enrich data from HANA and MySQL databases
- Process inventory tracking data via Kerberos-secured connections
- Write results to downstream systems

## Project Structure

```
java-projects/
├── flink/                           # Main Flink streaming application
│   ├── MainClass.java              # Application entry point
│   ├── JSONmapping/                # JSON serialization/deserialization
│   │   ├── CustomKafkaDeserializationSchema.java
│   │   ├── ProducerStringSerializationSchema.java
│   │   ├── adhocinv/               # Invoice models
│   │   │   ├── BUYER.java
│   │   │   ├── EMAIL.java
│   │   │   ├── INVOICE_DOC.java
│   │   │   ├── PHONE.java
│   │   │   └── invoice_master.java
│   │   └── kerbkafka/              # Kerberos-secured models
│   │       └── S_JMT_SANDSTORM_INVENTORY.java
│   └── Streaming/                  # Flink streaming operators
│       ├── JDBCSink.java           # Database sink
│       ├── adhocinv/
│       │   └── invoice.java        # Invoice processing job
│       └── kerbkafka/
│           └── InvAudGrp1.java     # Inventory audit group processing
├── pom.xml                         # Maven configuration
└── README.md                       # This file
```

## Prerequisites

### Software Requirements
- Java 8 or higher
- Maven 3.6+
- Apache Flink 1.14+
- Kafka 2.4+
- MySQL 5.7+ or compatible database
- SAP HANA (for some jobs)

### System Requirements
- Linux/macOS or Windows with Git Bash
- 4GB+ RAM (for local development)
- Stable network connection

## Installation & Setup

### 1. Clone Repository
```bash
git clone https://github.com/redshift7/java-projects.git
cd java-projects
```

### 2. Configure Environment Variables

Create a `.env` file in the project root with required credentials:

```bash
# Database Configuration
export DB_URL="jdbc:mysql://your-host:3306/database_name?allowPublicKeyRetrieval=true&useSSL=false"
export DB_USER="your_username"
export DB_PASSWORD="your_password"

# HANA Configuration (if using HANA database)
export HANA_URL="hana://your-hana-host:30015"
export HANA_USER="hana_user"
export HANA_PASSWORD="hana_password"

# Kerberos Configuration
export KERBEROS_PRINCIPAL="your_principal@YOUR.REALM.COM"
export KERBEROS_KEYTAB_PATH="/path/to/your.keytab"
export SSL_TRUSTSTORE_PATH="/path/to/truststore.jks"
export SSL_TRUSTSTORE_PASSWORD="truststore_password"

# Kafka Configuration
export KAFKA_BROKERS="kafka-broker-1:9092,kafka-broker-2:9092"
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"

# Logging
export LOG_LEVEL="INFO"
```

### 3. Build Project
```bash
# Build with Maven
mvn clean package
```

## Running the Application

### Local Development
```bash
# Run locally with default Kafka settings
mvn exec:java -Dexec.mainClass="com.example.flink.MainClass"

# Or run the JAR
java -jar target/invoice-streaming.jar
```

### Submit to Flink Cluster
```bash
# Standalone cluster
flink run target/invoice-streaming.jar

# YARN cluster
flink run -m yarn -yn 4 -yjm 1024m -ytm 2048m target/invoice-streaming.jar

# Kubernetes
flink run-application -t kubernetes-application target/invoice-streaming.jar
```

## Configuration

### Environment Variables (Required)

| Variable | Description | Example |
|----------|-------------|---------|
| `DB_URL` | MySQL database connection URL | `jdbc:mysql://localhost:3306/invoices` |
| `DB_USER` | Database username | `app_user` |
| `DB_PASSWORD` | Database password | `[secure-password]` |
| `KERBEROS_PRINCIPAL` | Kerberos principal name | `app@YOUR.REALM` |
| `KERBEROS_KEYTAB_PATH` | Path to Kerberos keytab file | `/etc/security/keytabs/app.keytab` |
| `SSL_TRUSTSTORE_PATH` | Path to SSL truststore | `/etc/ssl/truststore.jks` |
| `KAFKA_BROKERS` | Kafka broker addresses | `kafka:9092` |

## Data Flow

1. **Source**: Kafka topic for invoices
   - Consumes invoice event JSON
   - Handles schema evolution

2. **Processing**: Stream processing pipeline
   - Deserialize JSON events
   - Validate data quality
   - Enrich with HANA/MySQL lookups
   - Handle errors and duplicates

3. **Sink**: Multiple sinks
   - Write to MySQL for analytics
   - Send to Kafka for downstream
   - Write to HDFS for archival

## Module Details

### MainClass.java
Entry point for the Flink application. Sets up:
- Kafka consumer
- Stream processing pipeline
- Sinks and error handling

### JSONmapping/
Classes for JSON serialization/deserialization:
- **CustomKafkaDeserializationSchema**: Custom deserialization logic
- **ProducerStringSerializationSchema**: Serialization for output
- **adhocinv/**: Invoice-specific models (BUYER, EMAIL, PHONE, INVOICE_DOC, invoice_master)
- **kerbkafka/**: Inventory models (S_JMT_SANDSTORM_INVENTORY)

### Streaming/
Flink streaming operators:
- **JDBCSink.java**: Custom sink for writing to JDBC databases
- **invoice.java**: Main invoice processing job
- **InvAudGrp1.java**: Inventory audit group processing with Kerberos security

## Security Considerations

### ⚠️ Important Security Notes

1. **Never commit secrets**: All credentials must be:
   - Set via environment variables
   - Managed by secure configuration systems
   - Never hardcoded in source files

2. **Credential Management**:
   - Use `.env` file locally (in `.gitignore`)
   - Use secrets managers in production
   - Rotate credentials regularly

3. **Network Security**:
   - Use SSL/TLS for all connections
   - Enable Kerberos authentication
   - Restrict network access to systems

4. **Database Access**:
   - Use principle of least privilege
   - Create service accounts with minimal permissions
   - Audit database access logs

## Building for Production

### Create Distribution Package
```bash
# Build optimized JAR for production
mvn clean package -DskipTests -Pproduction

# Output: target/invoice-streaming.jar
```

## License

[Add your license]

---

**Last Updated**: 2026-04-16
**Version**: 1.0.0
