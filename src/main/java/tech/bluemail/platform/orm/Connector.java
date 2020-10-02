package tech.bluemail.platform.orm;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import tech.bluemail.platform.exceptions.DatabaseException;
import tech.bluemail.platform.logging.Logger;

public class Connector {
    private DataSource dataSource;

    private String key;

    private String databaseName;

    private String host;

    private int port;

    private String username;

    private String password;

    public synchronized void iniDataSource() throws Exception {
        switch (getDriver()) {
            case "mysql":
                this.dataSource = new DataSource("com.mysql.jdbc.Driver", "jdbc:mysql://" + getHost() + ":" + getPort() + "/" + getName(), getUsername(), getPassword());
                return;
            case "pgsql":
                this.dataSource = new DataSource("org.postgresql.Driver", "jdbc:postgresql://" + getHost() + ":" + getPort() + "/" + getName(), getUsername(), getPassword());
                return;
        }
        throw new DatabaseException("Database Not Supported !");
    }

    public synchronized List<LinkedHashMap<String, Object>> executeQuery(String query, Object[] data, int returnType) throws DatabaseException {
        List<LinkedHashMap<String, Object>> results = new ArrayList();
        try {
            Connection connection = this.dataSource.getConnection();
            try {
                PreparedStatement pr = connection.prepareStatement(query, 1005, 1008);
                try {
                    if (data != null && data.length > 0) {
                        int index = 1;
                        int type = 0;
                        for (Object object : data) {
                            if (object != null) {
                                switch (object.getClass().getName()) {
                                    case "java.lang.String":
                                        type = 12;
                                        break;
                                    case "java.lang.Double":
                                        type = 3;
                                        break;
                                    case "java.lang.Integer":
                                        type = 4;
                                        break;
                                    case "java.sql.Date":
                                        type = 91;
                                        break;
                                    case "java.sql.Timestamp":
                                        type = 93;
                                        break;
                                    case "java.lang.Boolean":
                                        type = 16;
                                        break;
                                }
                                pr.setObject(index, object, type);
                                index++;
                            }
                        }
                    }
                    ResultSet result = pr.executeQuery();
                    try {
                        if (result.isBeforeFirst()) {
                            LinkedHashMap<String, Object> row;
                            int i;
                            ResultSetMetaData meta = result.getMetaData();
                            int count = 0;
                            switch (returnType) {
                                case 1:
                                    while (result.next()) {
                                        LinkedHashMap<String, Object> linkedHashMap = new LinkedHashMap<>();
                                        for (int j = 1; j <= meta.getColumnCount(); j++)
                                            linkedHashMap.put(meta.getColumnName(j), result.getObject(j));
                                        results.add(linkedHashMap);
                                        count++;
                                    }
                                    break;
                                case 0:
                                    result.first();
                                    row = new LinkedHashMap<>();
                                    for (i = 1; i <= meta.getColumnCount(); i++)
                                        row.put(meta.getColumnName(i), result.getObject(i));
                                    results.add(row);
                                    count++;
                                    break;
                            }
                            this.affectedRowsCount = count;
                        }
                        if (result != null)
                            result.close();
                    } catch (Throwable throwable) {
                        if (result != null)
                            try {
                                result.close();
                            } catch (Throwable throwable1) {
                                throwable.addSuppressed(throwable1);
                            }
                        throw throwable;
                    }
                    if (pr != null)
                        pr.close();
                } catch (Throwable throwable) {
                    if (pr != null)
                        try {
                            pr.close();
                        } catch (Throwable throwable1) {
                            throwable.addSuppressed(throwable1);
                        }
                    throw throwable;
                }
                if (connection != null)
                    connection.close();
            } catch (Throwable throwable) {
                if (connection != null)
                    try {
                        connection.close();
                    } catch (Throwable throwable1) {
                        throwable.addSuppressed(throwable1);
                    }
                throw throwable;
            }
        } catch (Exception e) {
            this.lastErrorMessage = e.getMessage();
            throw new DatabaseException(e);
        }
        return results;
    }

    public synchronized int executeUpdate(String query, Object[] data, int returnType) throws DatabaseException {
        int result = 0;
        try {
            Connection connection = this.dataSource.getConnection();
            try {
                PreparedStatement pr = (returnType == 1) ? connection.prepareStatement(query, 1) : connection.prepareStatement(query);
                try {
                    if (data != null && data.length > 0) {
                        int index = 1;
                        int type = 0;
                        for (Object object : data) {
                            if (object != null)
                                switch (object.getClass().getName()) {
                                    case "java.lang.String":
                                        type = 12;
                                        break;
                                    case "java.lang.Double":
                                        type = 3;
                                        break;
                                    case "java.lang.Integer":
                                        type = 4;
                                        break;
                                    case "java.sql.Date":
                                        type = 91;
                                        break;
                                    case "java.sql.Timestamp":
                                        type = 93;
                                        break;
                                    case "java.lang.Boolean":
                                        type = 16;
                                        break;
                                }
                            pr.setObject(index, object, type);
                            index++;
                        }
                    }
                    result = pr.executeUpdate();
                    if (returnType == 1) {
                        ResultSet rs = pr.getGeneratedKeys();
                        if (rs.next()) {
                            this.lastInsertedId = rs.getInt(1);
                            result = this.lastInsertedId;
                        }
                        closeResultset(rs);
                    } else {
                        this.affectedRowsCount = result;
                    }
                    if (pr != null)
                        pr.close();
                } catch (Throwable throwable) {
                    if (pr != null)
                        try {
                            pr.close();
                        } catch (Throwable throwable1) {
                            throwable.addSuppressed(throwable1);
                        }
                    throw throwable;
                }
                if (connection != null)
                    connection.close();
            } catch (Throwable throwable) {
                if (connection != null)
                    try {
                        connection.close();
                    } catch (Throwable throwable1) {
                        throwable.addSuppressed(throwable1);
                    }
                throw throwable;
            }
        } catch (Exception e) {
            this.lastErrorMessage = e.getMessage();
            throw new DatabaseException(e);
        }
        return result;
    }

    public synchronized List<String> availableTables(String schema) throws Exception {
        List<String> tables = new ArrayList<>();
        String sql = "";
        String condition = (schema != null && !"".equals(schema)) ? ("WHERE schemaname = '" + schema + "'") : "";
        String columns = (schema != null && !"".equals(schema)) ? "relname" : "schemaname || '.' || relname";
        if ("pgsql".equalsIgnoreCase(this.driver)) {
            sql = "SELECT " + columns + " AS name FROM pg_stat_user_tables " + condition + " ORDER BY name ASC";
        } else if ("mysql".equalsIgnoreCase(this.driver)) {
            sql = "SHOW tables FROM " + this.databaseName;
        }
        List<LinkedHashMap<String, Object>> result = executeQuery(sql, null, 1);
        result.forEach(row -> tables.add(String.valueOf(row.get("name"))));
        return tables;
    }

    public synchronized void transaction(int type) {
        this.lastErrorMessage = "";
        try {
            switch (type) {
                case 0:
                    this.dataSource.getConnection().setSavepoint();
                    return;
                case 1:
                    this.dataSource.getConnection().commit();
                    return;
                case 2:
                    this.dataSource.getConnection().rollback();
                    return;
            }
            this.lastErrorMessage = "The passed transaction type is wrong!";
            throw new DatabaseException(this.lastErrorMessage);
        } catch (Exception e) {
            this.lastErrorMessage = e.getMessage();
            Logger.error(e, Connector.class);
        }
    }

    public synchronized void closePreparedStatement(PreparedStatement pr) {
        try {
            if (pr != null)
                pr.close();
        } catch (SQLException e) {
            this.lastErrorMessage = e.getMessage();
            Logger.error(e, Connector.class);
        }
    }



    public synchronized void closeResultset(ResultSet result) {
        try {
            if (result != null)
                result.close();
        } catch (SQLException e) {
            this.lastErrorMessage = e.getMessage();
            Logger.error(e, Connector.class);
        }
    }

    public Query query() {
        return new Query(this.key);
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getName() {
        return this.databaseName;
    }

    public void setName(String name) {
        this.databaseName = name;
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriver() {
        return this.driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getCharset() {
        return this.charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getEngine() {
        return this.engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String[] getSupportedDrivers() {
        return this.supportedDrivers;
    }

    public void setSupportedDrivers(String[] supportedDrivers) {
        this.supportedDrivers = supportedDrivers;
    }

    public String getLastErrorMessage() {
        return this.lastErrorMessage;
    }

    public void setLastErrorMessage(String lastErrorMessage) {
        this.lastErrorMessage = lastErrorMessage;
    }

    public int getLastInsertedId() {
        return this.lastInsertedId;
    }

    public void setLastInsertedId(int lastInsertedId) {
        this.lastInsertedId = lastInsertedId;
    }

    public int getAffectedRowsCount() {
        return this.affectedRowsCount;
    }

    public void setAffectedRowsCount(int affectedRowsCount) {
        this.affectedRowsCount = affectedRowsCount;
    }

    public DataSource getDataSource() {
        return this.dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    private String driver = "mysql";

    private String charset = "utf8";

    private String engine = "InnoDB";

    private String[] supportedDrivers = new String[] { "mysql", "pgsql" };

    private String lastErrorMessage = "";

    private int lastInsertedId = 0;

    private int affectedRowsCount = 0;

    public static final int FETCH_FIRST = 0;

    public static final int FETCH_ALL = 1;

    public static final int FETCH_LAST = 3;

    public static final int AFFECTED_ROWS = 0;

    public static final int LAST_INSERTED_ID = 1;

    public static final int BEGIN_TRANSACTION = 0;

    public static final int COMMIT_TRANSACTION = 1;

    public static final int ROLLBACK_TRANSACTION = 2;
}
