package com.antelope.com.sql2rel;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.SourceStringReader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class Sql2Rel
{
    public static void main( String[] args ) throws Exception
    {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");

        Connection connection = DriverManager.getConnection("jdbc:calcite:",info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        final DataSource ds = JdbcSchema.dataSource(
                "jdbc:mysql://localhost:3306/poc",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "rimpA@5536");
        rootSchema.add("MULTIDB", JdbcSchema.create(rootSchema, "MULTIDB", ds, null, null));
        System.out.println(rootSchema.toString());

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        // Ponasanje planner-a definira se konfiguracijom. U njoj
        // se definiraju rulovi, sheme i sve ostalo.
        Planner planner = Frameworks.getPlanner(config);

        SqlNode sqlNode = planner.parse("show tables");
        System.out.println(sqlNode.toString());

        sqlNode = planner.validate(sqlNode);

        RelRoot relRoot = planner.rel(sqlNode);
        System.out.println(relRoot.toString());

        RelNode relNode = relRoot.project();
        System.out.println(RelOptUtil.toString(relNode));

    }
}
