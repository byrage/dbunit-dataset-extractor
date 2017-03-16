package byrage.extractor.model;

import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public class Config {

    private static final String PROPERTY_TYPE = "TYPE";
    private static final String PROPERTY_HOST = "HOST";
    private static final String PROPERTY_PORT = "PORT";
    private static final String PROPERTY_DB_NAME = "DB_NAME";
    private static final String PROPERTY_ID = "ID";
    private static final String PROPERTY_PASSWORD = "PASSWORD";
    private static final String PROPERTY_TABLES_AND_QUERIES = "TABLES_AND_QUERIES";
    private static final String PROPERTY_OUTPUT_FILE_NAME = "OUTPUT_FILE_NAME";

    private DbType type;
    private String host;
    private String port;
    private String dbName;
    private String id;
    private String password;
    private String[] tablesAndQueries;
    private String outputFileName;

    public Config(Properties prop) {

        setType(prop.getProperty(PROPERTY_TYPE));
        setHost(prop.getProperty(PROPERTY_HOST));
        setPort(prop.getProperty(PROPERTY_PORT));
        setDbName(prop.getProperty(PROPERTY_DB_NAME));
        setId(prop.getProperty(PROPERTY_ID));
        setPassword(prop.getProperty(PROPERTY_PASSWORD));
        setTablesAndQueries(prop.getProperty(PROPERTY_TABLES_AND_QUERIES));
        setOutputFileName(prop.getProperty(PROPERTY_OUTPUT_FILE_NAME));
    }

    public DbType getType() {

        return type;
    }

    public void setType(String type) {

        this.type = DbType.getDbTypeByString(type);
    }

    public String getHost() {

        return host;
    }

    public void setHost(String host) {

        this.host = host;
    }

    public String getPort() {

        return port;
    }

    public void setPort(String port) {

        this.port = port;
    }

    public String getDbName() {

        return dbName;
    }

    public void setDbName(String dbName) {

        this.dbName = dbName;
    }

    public String getId() {

        return id;
    }

    public void setId(String id) {

        this.id = id;
    }

    public String getPassword() {

        return password;
    }

    public void setPassword(String password) {

        this.password = password;
    }

    public String[] getTablesAndQueries() {

        return tablesAndQueries;
    }

    public void setTablesAndQueries(String tablesAndQueries) {

        this.tablesAndQueries = StringUtils.split(tablesAndQueries, "/");
    }

    public String getOutputFileName() {

        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {

        this.outputFileName = outputFileName;
    }
}
