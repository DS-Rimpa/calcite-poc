package com.antelope.com.jdbcproxy;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class Main {

    /*
     * Argument is a model.json file
     */
    public static void main (String[] argv) throws Exception {

//        Class.forName(org.apache.calcite.jdbc.Driver.class.getName());
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
//        info.setProperty("lex", "JAVA");
//        info.setProperty("model", argv[0]);
        info.setProperty("lex", "MYSQL");
//        Connection calConnection = DriverManager.getConnection("jdbc:calcite:", info);
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();

    }
}
