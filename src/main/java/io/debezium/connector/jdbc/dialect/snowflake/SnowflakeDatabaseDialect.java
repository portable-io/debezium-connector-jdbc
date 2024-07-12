package io.debezium.connector.jdbc.dialect.snowflake;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.DerbyDialect;
import org.hibernate.dialect.Dialect;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;

public class SnowflakeDatabaseDialect extends GeneralDatabaseDialect {

    public static class SnowflakeDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof DerbyDialect; // TODO: double-check. Suggested here: https://www.cdata.com/kb/tech/snowflake-jdbc-hibernate.rst
        }

        @Override
        public Class<?> name() {
            return SnowflakeDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new SnowflakeDatabaseDialect(config, sessionFactory);
        }
    }

    public SnowflakeDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    @Override
    protected boolean isIdentifierUppercaseWhenNotQuoted() {
        return true;
    }

    @Override
    protected Optional<String> getDatabaseTimeZoneQuery() {
        return Optional.of("SHOW PARAMETERS LIKE 'TIMEZONE'");
    }

    @Override
    protected String getDatabaseTimeZoneQueryResult(ResultSet rs) throws SQLException {
        return rs.getString("value");
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        return 16777216;
    }

    @Override
    public int getMaxNVarcharLengthInKey() {
        return 16777216;
    }

    @Override
    public int getMaxVarbinaryLength() {
        return 8388608;
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("MERGE INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" USING (SELECT ");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(),
                (name) -> columnQueryBindingFromField(name, table, record) + " " + columnNameFromField(name, record));
        builder.append(" FROM ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" ) AS INCOMING ON (");
        builder.appendList(" AND ", record.getKeyFieldNames(), (name) -> getUpsertIncomingClause(name, table, record));
        builder.append(")");
        if (!record.getNonKeyFieldNames().isEmpty()) {
            builder.append(" WHEN MATCHED THEN UPDATE SET ");
            builder.appendList(",", record.getNonKeyFieldNames(), (name) -> getUpsertIncomingClause(name, table, record));
        }
        builder.append(" WHEN NOT MATCHED THEN INSERT (");
        builder.appendLists(",", record.getNonKeyFieldNames(), record.getKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") VALUES (");
        builder.appendLists(",", record.getNonKeyFieldNames(), record.getKeyFieldNames(), (name) -> columnNameFromField(name, "INCOMING.", record));
        builder.append(")");
        return builder.build();
    }

    private String getUpsertIncomingClause(String fieldName, TableDescriptor table, SinkRecordDescriptor record) {
        final String columnName = columnNameFromField(fieldName, record);
        return toIdentifier(table.getId()) + "." + columnName + "=INCOMING." + columnName;
    }

    @Override
    public String getFormattedDate(TemporalAccessor value) {
        return String.format("%s:date", super.getFormattedDate(value));
    }

    @Override
    public String getFormattedTime(TemporalAccessor value) {
        return String.format("%s:time", super.getFormattedTime(value));
    }

    @Override
    public String getFormattedTimeWithTimeZone(String value) {
        return String.format("%s:time", super.getFormattedTimeWithTimeZone(value));
    }

    @Override
    public String getFormattedDateTime(TemporalAccessor value) {
        return String.format("%s:timestamp", super.getFormattedDateTime(value));
    }

    @Override
    public String getFormattedDateTimeWithNanos(TemporalAccessor value) {
        return getFormattedDateTime(value);
    }

    @Override
    public String getFormattedTimestamp(TemporalAccessor value) {
        return String.format("%s:timestamp", super.getFormattedTimestamp(value));
    }

    @Override
    public String getFormattedTimestampWithTimeZone(String value) {
        return String.format("%s:timestamp_tz", super.getFormattedTimestampWithTimeZone(value));
    }
}
