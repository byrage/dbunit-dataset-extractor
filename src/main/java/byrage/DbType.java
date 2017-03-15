package byrage;

public enum DbType {

    MySQL("com.mysql.jdbc.Driver", "jdbc:mysql://"),
    Oracle("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@");

    private String driverName;
    private String jdbcPrefix;

    private DbType(String driverName, String jdbcPrefix) {

        this.driverName = driverName;
        this.jdbcPrefix = jdbcPrefix;
    }

    public String getDriverName() {

        return driverName;
    }

    public String getJdbcPrefix() {

        return jdbcPrefix;
    }
}
