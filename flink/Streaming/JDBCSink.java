package com.example.flink.streaming;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.*;

public class JDBCSink extends RichSinkFunction<Row> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    /**
     * Constructor to set up the database connection properties.
     * 
     * SECURITY NOTE: Credentials should NOT be hardcoded in source code.
     * Instead, pass them from secure external sources:
     * - Environment variables
     * - Secure credential management systems (e.g., AWS Secrets Manager, HashiCorp Vault)
     * - Configuration files with restricted access (not in repository)
     * 
     * @param dbUrl Database URL (e.g., "jdbc:mysql://localhost:3306?allowPublicKeyRetrieval=true&useSSL=false")
     * @param dbUser Database username - must be passed securely, not hardcoded
     * @param dbPassword Database password - must be passed securely, not hardcoded
     */
    public JDBCSink(String dbUrl, String dbUser, String dbPassword) {
        validateCredentials(dbUrl, dbUser, dbPassword);
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    private void validateCredentials(String dbUrl, String dbUser, String dbPassword) {
        if (dbUrl == null || dbUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Database URL cannot be null or empty");
        }
        if (dbUser == null || dbUser.trim().isEmpty()) {
            throw new IllegalArgumentException("Database username cannot be null or empty");
        }
        if (dbPassword == null || dbPassword.trim().isEmpty()) {
            throw new IllegalArgumentException("Database password cannot be null or empty");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Open the database connection
        connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);

        // Prepare the SQL statement for inserting data
        String sql = "INSERT INTO database_table (`key`,`table`,nodeKeyRef,lastModifiedDate,email_id,`number`) VALUES ( ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE `key` = VALUES(`key`)";
        preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        // Set the values for the prepared statement
        preparedStatement.setString(1, row.getField(0).toString());
        preparedStatement.setString(2, row.getField(1).toString());
        preparedStatement.setString(3, row.getField(2).toString());
        preparedStatement.setTimestamp(4, (Timestamp) row.getField(3));
        preparedStatement.setString(5, row.getField(4) != null ? row.getField(4).toString() : null);
        preparedStatement.setString(6, row.getField(5) != null ? row.getField(5).toString() : null);

        // Execute the prepared statement to insert the data into the database
        preparedStatement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        // Close the database connection
        if (connection != null) {
            connection.close();
        }

        // Close the prepared statement
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }
}





