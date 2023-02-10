package com.antelope.com.rewriter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

public class EmployeesTableFactory implements TableFactory<Table> {
    @Override
    public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
        final Object[][] rows = {
                {1, "data1", 10},
                {2, "data2", 5},
                {3, "data3", 12},
                {4, "data4", 3},
                {5, "data5", 3}
        };
        return new EmployeeTable(ImmutableList.copyOf(rows));
    }

    public static class EmployeeTable implements ScannableTable {
        protected final RelProtoDataType protoRowType = new RelProtoDataType() {
            public RelDataType apply(RelDataTypeFactory a0) {
                return a0.builder()
                        .add("id", SqlTypeName.INTEGER)
                        .add("employee_names", SqlTypeName.VARCHAR, 10)
                        .add("work_days", SqlTypeName.INTEGER)
                        .build();
            }
        };

        private final ImmutableList<Object[]> rows;

        public EmployeeTable(ImmutableList<Object[]> rows) {
            this.rows = rows;
        }

        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(rows);
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoRowType.apply(typeFactory);
        }

        @Override
        public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }

        @Override
        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        @Override
        public boolean isRolledUp(String s) {
            return false;
        }

        @Override
        public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode, CalciteConnectionConfig calciteConnectionConfig) {
            return false;
        }
    }
}