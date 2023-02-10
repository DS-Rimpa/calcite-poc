package com.antelope.com;

import com.google.common.io.Resources;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SimpleQueryPlanner {
    private final Planner planner;

    public SimpleQueryPlanner(SchemaPlus schema) {
        final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);

        FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                       .setLex(Lex.MYSQL)
                        .build())
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .context(Contexts.EMPTY_CONTEXT)
                .ruleSets(RuleSets.ofList())
                .costFactory(null)
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .build();

        this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
    }

    public RelNode get(String query) throws ValidationException, RelConversionException {
        SqlNode sqlNode;

        try {
            sqlNode = planner.parse(query);
        } catch (SqlParseException e) {
            throw new RuntimeException("Query parsing error", e);
        }

        SqlNode validatedSqlNode = planner.validate(sqlNode);

        return planner.rel(validatedSqlNode).project();
    }

    public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException {
        CalciteConnection connection = (CalciteConnection) DriverManager.getConnection("jdbc:calcite:");
        String salesSchema = Resources.toString(SimpleQueryPlanner.class.getResource("/employees.json"), Charset.defaultCharset());
        new ModelHandler(connection, "inline:" + salesSchema);

        SimpleQueryPlanner queryPlanner = new SimpleQueryPlanner(connection.getRootSchema().getSubSchema(connection.getSchema()));
        RelNode logicalPlan = queryPlanner.get("select employee_names from employees");
        System.out.println("..."+RelOptUtil.toString(logicalPlan));
    }
}
