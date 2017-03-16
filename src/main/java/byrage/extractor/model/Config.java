package byrage.extractor.model;

import lombok.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

@Data
public class Config {

    private static final String PROPERTY_TYPE = "TYPE";
    private static final String PROPERTY_HOST = "HOST";
    private static final String PROPERTY_PORT = "PORT";
    private static final String PROPERTY_DB_NAME = "DB_NAME";
    private static final String PROPERTY_ID = "ID";
    private static final String PROPERTY_PASSWORD = "PASSWORD";
    private static final String PROPERTY_TABLES_AND_QUERIES = "TABLES_AND_QUERIES";
    private static final String PROPERTY_OUTPUT_FILE_NAME = "OUTPUT_FILE_NAME";

    @Setter(AccessLevel.NONE)
    private DbType type;

    private String host;

    private String port;

    private String dbName;

    private String id;

    private String password;

    @Setter(AccessLevel.NONE)
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

    public void setType(String type) {

        this.type = DbType.getDbTypeByString(type);
    }

    public void setTablesAndQueries(String tablesAndQueries) {

        this.tablesAndQueries = StringUtils.split(tablesAndQueries, "/");
    }
}
