package byrage.extractor.model;

import lombok.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

@Getter
public class Config {

    private static final String PROPERTY_TYPE = "TYPE";
    private static final String PROPERTY_HOST = "HOST";
    private static final String PROPERTY_PORT = "PORT";
    private static final String PROPERTY_DB_NAME = "DB_NAME";
    private static final String PROPERTY_ID = "ID";
    private static final String PROPERTY_PASSWORD = "PASSWORD";
    private static final String PROPERTY_TABLES_AND_QUERIES = "TABLES_AND_QUERIES";
    private static final String PROPERTY_OUTPUT_FILE_NAME = "OUTPUT_FILE_NAME";

    private String outputFileName;
    private String host;
    private String port;
    private String dbName;
    private String id;
    private String password;

    private DbType type;
    private String url;
    private String[] tablesAndQueries;

    public Config(Properties prop) {

        this.outputFileName = prop.getProperty(PROPERTY_OUTPUT_FILE_NAME);
        this.host = (prop.getProperty(PROPERTY_HOST));
        this.port = (prop.getProperty(PROPERTY_PORT));
        this.dbName = (prop.getProperty(PROPERTY_DB_NAME));
        this.id = (prop.getProperty(PROPERTY_ID));
        this.password = (prop.getProperty(PROPERTY_PASSWORD));

        this.setType(prop.getProperty(PROPERTY_TYPE));
        this.setUrl();
        this.setTablesAndQueries(prop.getProperty(PROPERTY_TABLES_AND_QUERIES));
    }

    private void setUrl() {

        String format;
        if (this.type.equals(DbType.MsSql)) {
            format = "%s%s:%s;database=%s";
        } else {
            format = "%s%s:%s/%s";
        }

        this.url = String.format(format, this.type.getJdbcPrefix(), this.host, this.port, this.dbName);
    }

    public void setType(String type) {

        this.type = DbType.getDbTypeByString(type);
    }

    public void setTablesAndQueries(String tablesAndQueries) {

        this.tablesAndQueries = StringUtils.split(tablesAndQueries, "/");
    }
}
