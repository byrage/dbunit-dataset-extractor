package byrage.extractor.model;

import org.apache.commons.lang3.StringUtils;
import org.dbunit.dataset.datatype.DefaultDataTypeFactory;
import org.dbunit.ext.mssql.MsSqlDataTypeFactory;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;
import org.dbunit.ext.oracle.OracleDataTypeFactory;

public enum DbType {

    MySQL("com.mysql.jdbc.Driver", "jdbc:mysql://", MySqlDataTypeFactory.class),
    Oracle("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@", OracleDataTypeFactory.class),
    MsSql("com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://", MsSqlDataTypeFactory.class);

    private String driverName;
    private String jdbcPrefix;
    private Class<? extends DefaultDataTypeFactory> dataTypeFactoryClass;

    private DbType(String driverName, String jdbcPrefix, Class<? extends DefaultDataTypeFactory> dataTypeFactoryClass) {

        this.driverName = driverName;
        this.jdbcPrefix = jdbcPrefix;
        this.dataTypeFactoryClass = dataTypeFactoryClass;
    }

    public String getDriverName() {

        return driverName;
    }

    public String getJdbcPrefix() {

        return jdbcPrefix;
    }

    public Class<? extends DefaultDataTypeFactory> getDataTypeFactoryClass() {

        return dataTypeFactoryClass;
    }

    public static DbType getDbTypeByString(String type) throws IllegalArgumentException {

        for (DbType dbType : DbType.values()) {
            if (StringUtils.equalsIgnoreCase(dbType.name(), type.trim())) {
                return dbType;
            }
        }

        throw new IllegalArgumentException("doesn't support db type. type=" + type.trim());
    }
}
