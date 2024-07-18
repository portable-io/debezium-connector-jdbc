package io.debezium.connector.jdbc.dialect.snowflake;

import org.hibernate.dialect.DatabaseVersion;

public class SnowflakeHibernateDialect extends org.hibernate.dialect.PostgreSQLDialect {
    @Override
    public DatabaseVersion getVersion() {
        return new DatabaseVersion() {
            @Override
            public int getDatabaseMajorVersion() {
                return 0;
            }

            @Override
            public int getDatabaseMinorVersion() {
                return 0;
            }
        };
    }
}
