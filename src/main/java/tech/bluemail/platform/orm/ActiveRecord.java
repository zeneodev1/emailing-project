package tech.bluemail.platform.orm;

import java.io.File;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import tech.bluemail.platform.exceptions.DatabaseException;
import tech.bluemail.platform.logging.Logger;
import tech.bluemail.platform.meta.annotations.Column;
import tech.bluemail.platform.utils.Inspector;

public class ActiveRecord {
    private String database;

    private String schema;

    private String table;

    private LinkedHashMap<String, LinkedHashMap<String, Object>> columns;

    private LinkedHashMap<String, Object> primary;

    public static final String INT = "integer";

    public static final String DECIMAL = "decimal";

    public static final String TEXT = "text";

    public static final String DATE = "date";

    public static final String TIME_STAMP = "timestamp";

    public static final String BOOL = "boolean";

    public static final int CREATE_TABLE = 0;

    public static final int CREATE_CLASS = 1;

    public static final int FETCH_ARRAY = 2;

    public static final int FETCH_OBJECT = 3;

    public static final int OBJECTS_ROWS = 4;

    public static final int ARRAYS_ROWS = 5;

    public ActiveRecord() throws DatabaseException {
        this.database = "";
        this.schema = "";
        this.table = "";
        this.columns = new LinkedHashMap<>();
        this.primary = new LinkedHashMap<>();
        init();
    }

    public ActiveRecord(Object primaryValue) throws DatabaseException {
        this.database = "";
        this.schema = "";
        this.table = "";
        this.columns = new LinkedHashMap<>();
        this.primary = new LinkedHashMap<>();
        init();
        setFieldValue((String)this.primary.get("field"), primaryValue);
    }

    public void load(String column, Object value) {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        String schema = (this.schema == null || "".equals(this.schema)) ? "" : (this.schema + ".");
        try {
            if (column != null && !"".equals(column) && value != null && !"".equals(value))
                row = Database.get(this.database).query().from(schema + this.table, new String[] { "*" }).where(column + " = ?", new Object[] { value }, "").first();
            if (!row.isEmpty())
                row.forEach((c, v) -> {
                    if (getColumns().containsKey(c)) {
                        String field = (String)((LinkedHashMap)getColumns().get(c)).get("field");
                        setFieldValue(field, v);
                    }
                });
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
    }

    public void load() {
        if (!this.primary.isEmpty()) {
            Object primaryValue = getFieldValue(String.valueOf(this.primary.get("field")));
            if (primaryValue != null)
                load(String.valueOf(this.primary.get("name")), primaryValue);
        }
    }

    public void unLoad() {
        this.columns.forEach((columnName, column) -> {
            if (columnName != null && column != null && !column.isEmpty()) {
                Object value = "integer".equalsIgnoreCase(String.valueOf(column.get("type"))) ? new Integer(0) : null;
                setFieldValue(String.valueOf(column.get("field")), value);
            }
        });
    }

    public void map(LinkedHashMap<String, Object> row) {
        if (!row.isEmpty())
            row.forEach((c, v) -> {
                if (getColumns().containsKey(c)) {
                    String field = (String)((LinkedHashMap)getColumns().get(c)).get("field");
                    setFieldValue(field, v);
                }
            });
    }

    public int insert() {
        int id = 0;
        try {
            boolean customPrimary = false;
            int index = 0;
            String schema = (this.schema == null || "".equals(this.schema)) ? "" : (this.schema + ".");
            String[] columns = new String[this.columns.size()];
            Object[] data = new Object[this.columns.size()];
            if (!this.primary.isEmpty()) {
                columns[index] = (String)this.primary.get("name");
                Object value = getFieldValue((String)this.primary.get("field"));
                if (value == null || Integer.parseInt(String.valueOf(value)) == 0) {
                    if ("pgsql".equalsIgnoreCase(Database.get(this.database).getDriver())) {
                        value = Integer.valueOf(0);
                        List<LinkedHashMap<String, Object>> nextVal = Database.get(this.database).executeQuery("SELECT nextval('" + schema + "seq_" + String.valueOf(this.primary.get("name")) + "_" + this.table + "')", null, 0);
                        if (!nextVal.isEmpty())
                            if (((LinkedHashMap)nextVal.get(0)).containsKey("nextval"))
                                value = Integer.valueOf(Integer.parseInt(String.valueOf(((LinkedHashMap)nextVal.get(0)).get("nextval"))));
                    } else {
                        value = null;
                    }
                    data[index] = value;
                } else {
                    if ("pgsql".equalsIgnoreCase(Objects.requireNonNull(Database.get(this.database)).getDriver()))
                        customPrimary = true;
                    data[index] = value;
                }
                index++;
            }
            Iterator<Map.Entry<String, LinkedHashMap<String, Object>>> it = this.columns.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = it.next();
                String columnName = (String)pair.getKey();
                LinkedHashMap<String, Object> column = (LinkedHashMap<String, Object>)pair.getValue();
                if (false == ((Boolean)column.get("primary")).booleanValue()) {
                    columns[index] = columnName;
                    data[index] = getFieldValue((String)column.get("field"));
                    index++;
                }
                it.remove();
            }
            id = Database.get(this.database).query().from(schema + this.table, columns).insert(data);
            if (customPrimary)
                Database.get(this.database).executeUpdate("ALTER SEQUENCE IF EXISTS " + schema + "seq_" + String.valueOf(this.primary.get("name")) + "_" + this.table + " RESTART WITH " + (id + 1), null, 0);
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return id;
    }

    public int update() {
        int affectedRows = 0;
        int index = 0;
        String schema = (this.schema == null || "".equals(this.schema)) ? "" : (this.schema + ".");
        String[] columns = new String[this.columns.size() - 1];
        Object[] data = new Object[this.columns.size() - 1];
        try {
            String primaryColumn = (String)this.primary.get("name");
            Object primaryValue = getFieldValue((String)this.primary.get("field"));
            if (primaryValue == null || Integer.parseInt(String.valueOf(primaryValue)) == 0)
                throw new DatabaseException("Primary Key Must Not Be Null !");
            Iterator<Map.Entry<String, LinkedHashMap<String, Object>>> it = this.columns.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = it.next();
                String columnName = (String)pair.getKey();
                LinkedHashMap<String, Object> column = (LinkedHashMap<String, Object>)pair.getValue();
                if (false == ((Boolean)column.get("primary")).booleanValue()) {
                    columns[index] = columnName;
                    data[index] = getFieldValue((String)column.get("field"));
                    index++;
                }
                it.remove();
            }
            affectedRows = Database.get(this.database).query().from(schema + this.table, columns).where(primaryColumn + " = ?", new Object[] { primaryValue }, "").update(data);
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return affectedRows;
    }

    public int delete() {
        int affectedRows = 0;
        try {
            String schema = (this.schema == null || "".equals(this.schema)) ? "" : (this.schema + ".");
            String primaryColumn = (String)this.primary.get("name");
            Object primaryValue = getFieldValue((String)this.primary.get("field"));
            if (primaryValue == null || Integer.parseInt(String.valueOf(primaryValue)) == 0)
                throw new DatabaseException("Primary Key Must Not Be Null !");
            affectedRows = Database.get(this.database).query().from(schema + this.table, new String[0]).where(primaryColumn + " = ?", new Object[] { primaryValue }, "").delete();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return affectedRows;
    }

    public boolean tableExists() {
        boolean res = false;
        try {
            List<LinkedHashMap<String, Object>> result = Database.get(this.database).executeQuery("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE  n.nspname = ? AND c.relname = ? AND c.relkind = 'r')", new Object[] { this.schema, this.table }, 0);
            if (result != null && !result.isEmpty())
                return ((Boolean)((LinkedHashMap)result.get(0)).get("exists")).booleanValue();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return res;
    }

    public int sync() {
        int result = 0;
        try {
            String schema = (this.schema == null || "".equals(this.schema)) ? "" : (this.schema + ".");
            Iterator<Map.Entry<String, LinkedHashMap<String, Object>>> it = this.columns.entrySet().iterator();
            String lines = "";
            String indice = "";
            String sequence = "";
            String line = "";
            while (it.hasNext()) {
                Map.Entry pair = it.next();
                LinkedHashMap<String, Object> column = (LinkedHashMap<String, Object>)pair.getValue();
                String name = (String)pair.getKey();
                boolean primary = ((Boolean)column.get("primary")).booleanValue();
                boolean autoIncrement = ((Boolean)column.get("autoincrement")).booleanValue();
                String type = (String)column.get("type");
                String nullable = ((Boolean)column.get("nullable")).booleanValue() ? " DEFAULT NULL " : " NOT NULL ";
                int length = ((Integer)column.get("length")).intValue();
                switch (type) {
                    case "integer":
                        line = name + " integer " + nullable;
                        if ("pgsql".equalsIgnoreCase(Database.get(this.database).getDriver())) {
                            if (primary)
                                indice = "CONSTRAINT c_pk_" + name + "_" + this.table + " PRIMARY KEY(" + name + ") \n";
                            if (autoIncrement)
                                sequence = "seq_" + name + "_" + this.table;
                        } else {
                            if (primary)
                                line = line + " PRIMARY KEY ";
                            if (autoIncrement)
                                line = line + " AUTO_INCREMENT ";
                        }
                        lines = lines + line + ",\n";
                        break;
                    case "decimal":
                        lines = lines + name + " decimal " + nullable + ",\n";
                        break;
                    case "text":
                        if (length > 0 && length <= 255) {
                            lines = lines + name + " varchar(" + length + ") " + nullable + ",\n";
                            break;
                        }
                        lines = lines + name + " text " + nullable + ",\n";
                        break;
                    case "boolean":
                        lines = lines + name + " boolean " + nullable + ",\n";
                        break;
                    case "timestamp":
                        lines = lines + name + " timestamp " + nullable + ",\n";
                        break;
                    case "date":
                        lines = lines + name + " timestamp " + nullable + ",\n";
                        break;
                }
                it.remove();
            }
            String query = String.format("CREATE TABLE IF NOT EXISTS %s (\n%s,\n%s);", new Object[] { schema + this.table, lines.substring(0, lines.length() - 2), indice });
            Database.get(this.database).executeUpdate(query, null, 0);
            if (!"".equalsIgnoreCase(sequence)) {
                String sequenceQuery = "CREATE SEQUENCE IF NOT EXISTS " + schema + sequence + " START 1";
                Database.get(this.database).executeUpdate(sequenceQuery, null, 0);
            }
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return result;
    }

    public static ActiveRecord first(Class<? extends ActiveRecord> subClass, String condition, Object[] values) {
        ActiveRecord record = null;
        try {
            ActiveRecord outerObject = subClass.getConstructor(new Class[0]).newInstance(new Object[0]);
            String schema = (outerObject.getSchema() == null || "".equals(outerObject.getSchema())) ? "" : (outerObject.getSchema() + ".");
            Query queryBuilder = Database.get(outerObject.getDatabase()).query().from(schema + outerObject.getTable(), new String[] { "*" }).order(new String[] { String.valueOf(outerObject.getPrimary().get("name")) }, "ASC");
            if (condition != null && !"".equalsIgnoreCase(condition))
                queryBuilder.where(condition, values, "");
            LinkedHashMap<String, Object> row = queryBuilder.first();
            if (row != null && !row.isEmpty()) {
                record = subClass.getConstructor(new Class[0]).newInstance(new Object[0]);
                Iterator<Map.Entry<String, Object>> it = row.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = it.next();
                    String columnName = (String)pair.getKey();
                    Object columnValue = pair.getValue();
                    if (record.getColumns().containsKey(columnName))
                        record.setFieldValue((String)((LinkedHashMap)record.getColumns().get(columnName)).get("field"), columnValue);
                    it.remove();
                }
            }
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return record;
    }

    public static ActiveRecord last(Class subClass) {
        return last(subClass, "", new Object[0]);
    }

    public static ActiveRecord last(Class<ActiveRecord> subClass, String condition, Object[] values) {
        ActiveRecord record = null;
        try {
            ActiveRecord outerObject = subClass.getConstructor(new Class[0]).newInstance(new Object[0]);
            String schema = (outerObject.getSchema() == null || "".equals(outerObject.getSchema())) ? "" : (outerObject.getSchema() + ".");
            Query queryBuilder = Database.get(outerObject.getDatabase()).query().from(schema + outerObject.getTable(), new String[] { "*" }).order(new String[] { String.valueOf(outerObject.getPrimary().get("name")) }, "DESC");
            if (condition != null && !"".equalsIgnoreCase(condition))
                queryBuilder.where(condition, values, "");
            LinkedHashMap<String, Object> row = queryBuilder.first();
            if (row != null && !row.isEmpty()) {
                record = subClass.getConstructor(new Class[0]).newInstance(new Object[0]);
                Iterator<Map.Entry<String, Object>> it = row.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = it.next();
                    String columnName = (String)pair.getKey();
                    Object columnValue = pair.getValue();
                    if (record.getColumns().containsKey(columnName))
                        record.setFieldValue((String)((LinkedHashMap)record.getColumns().get(columnName)).get("field"), columnValue);
                    it.remove();
                }
            }
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return record;
    }

    public static List all(Class subClass) {
        return all(subClass, "", new Object[0]);
    }

    public static List all(Class<? extends ActiveRecord> subClass, String condition, Object[] values) {
        List records = new ArrayList();
        try {
            ActiveRecord record = null;
            ActiveRecord outerObject = subClass.getConstructor(new Class[0]).newInstance(new Object[0]);
            String schema = (outerObject.getSchema() == null || "".equals(outerObject.getSchema())) ? "" : (outerObject.getSchema() + ".");
            Query queryBuilder = Database.get(outerObject.getDatabase()).query().from(schema + outerObject.getTable(), new String[] { "*" });
            if (condition != null && !"".equalsIgnoreCase(condition))
                queryBuilder.where(condition, values, "");
            List<LinkedHashMap<String, Object>> rows = queryBuilder.order(new String[] { String.valueOf(outerObject.getPrimary().get("name")) }, "ASC").all();
            if (rows != null && !rows.isEmpty())
                for (LinkedHashMap<String, Object> row : rows) {
                    record = subClass.getConstructor(new Class[0]).newInstance(new Object[0]);
                    Iterator<Map.Entry<String, Object>> it = row.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry pair = it.next();
                        String columnName = (String)pair.getKey();
                        Object columnValue = pair.getValue();
                        if (record.getColumns().containsKey(columnName))
                            record.setFieldValue((String)((LinkedHashMap)record.getColumns().get(columnName)).get("field"), columnValue);
                        it.remove();
                    }
                    records.add(subClass.cast(record));
                }
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return records;
    }

    public static int[] insert(List records) {
        int[] results = new int[0];
        if (records != null && records.size() > 0)
            for (Object record : records) {
                if (record != null)
                    results = ArrayUtils.add(results, ((ActiveRecord)record).insert());
            }
        return results;
    }

    public static int update(List records) {
        int affectedRows = 0;
        if (records != null && records.size() > 0)
            for (Object record : records) {
                if (record != null)
                    affectedRows = ((ActiveRecord)record).insert();
            }
        return affectedRows;
    }

    public static int delete(List records) {
        int affectedRows = 0;
        if (records != null && records.size() > 0)
            for (Object record : records) {
                if (record != null)
                    affectedRows = ((ActiveRecord)record).delete();
            }
        return affectedRows;
    }

    public static int delete(Class subClass) {
        return delete(subClass, "", new Object[0]);
    }

    public static int delete(Class<ActiveRecord> subClass, String condition, Object[] values) {
        int affectedRows = 0;
        try {
            ActiveRecord outerObject = subClass.getConstructor(new Class[0]).newInstance(new Object[0]);
            String schema = (outerObject.getSchema() == null || "".equals(outerObject.getSchema())) ? "" : (outerObject.getSchema() + ".");
            Query queryBuilder = Database.get(outerObject.getDatabase()).query().from(schema + outerObject.getTable(), new String[0]);
            if (condition != null && !"".equalsIgnoreCase(condition))
                queryBuilder.where(condition, values, "");
            affectedRows = queryBuilder.delete();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return affectedRows;
    }

    public static boolean sync(String model, String database, String schema, String table) {
        boolean created = false;
        try {
            String template = FileUtils.readFileToString(new File(ActiveRecord.class.getResource("/tech/bluemail/platform/templates/model.tpl").getPath()));
            template = StringUtils.replace(template, "$P{MODEL}", model);
            template = StringUtils.replace(template, "$P{SCHEMA}", schema);
            template = StringUtils.replace(template, "$P{TABLE}", table);
            template = StringUtils.replace(template, "$P{DATABASE}", database);
            String columns = "";
            String tab = "    ";
            List<LinkedHashMap<String, Object>> rows = Database.get(database).executeQuery("SELECT * FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position ASC", new Object[] { schema, table }, 1);
            if (rows != null && !rows.isEmpty())
                for (LinkedHashMap<String, Object> row : rows) {
                    if (row != null && !row.isEmpty()) {
                        String name = String.valueOf(row.get("column_name")).toLowerCase();
                        boolean primary = "id".equals(name);
                        String type = "character varying".equalsIgnoreCase(String.valueOf(row.get("data_type"))) ? "text" : String.valueOf(row.get("data_type")).toLowerCase();
                        type = type.contains("timestamp") ? "timestamp" : type;
                        int length = (!row.containsKey("character_maximum_length") || row.get("character_maximum_length") == null) ? 0 : Integer.parseInt(String.valueOf(row.get("character_maximum_length")));
                        boolean nullable = "YES".equalsIgnoreCase(String.valueOf(row.get("is_nullable")));
                        String javaType = "String";
                        char[] field = WordUtils.capitalize(name.replaceAll("_", " ").toLowerCase()).replaceAll(" ", "").toCharArray();
                        field[0] = Character.toLowerCase(field[0]);
                        switch (type) {
                            case "integer":
                                javaType = "int";
                                break;
                            case "text":
                                javaType = "String";
                                break;
                            case "decimal":
                                javaType = "double";
                                break;
                            case "boolean":
                                javaType = "boolean";
                                break;
                            case "date":
                                javaType = "java.sql.Date";
                                break;
                            case "timestamp":
                                javaType = "java.sql.Timestamp";
                                break;
                        }
                        columns = columns + "\n";
                        columns = columns + tab + "@Column\n";
                        columns = columns + tab + "(\n";
                        columns = columns + tab + tab + "name = \"" + name + "\",\n";
                        if (primary) {
                            columns = columns + tab + tab + "primary = true,\n";
                            columns = columns + tab + tab + "autoincrement = true,\n";
                        }
                        columns = columns + tab + tab + "type = \"" + type + "\",\n";
                        if (length > 0) {
                            columns = columns + tab + tab + "nullable = " + String.valueOf(nullable) + ",\n";
                            columns = columns + tab + tab + "length = " + length + "\n";
                        } else {
                            columns = columns + tab + tab + "nullable = " + String.valueOf(nullable) + "\n";
                        }
                        columns = columns + tab + ")\n";
                        columns = columns + tab + "public " + javaType + " " + new String(field) + ";\n";
                    }
                }
            template = StringUtils.replace(template, "$P{COLUMNS}", columns);
            FileUtils.writeStringToFile(new File((new File(System.getProperty("base.path"))).getParent() + File.separator + "src" + File.separator + "tech" + File.separator + "bluemail" + File.separator + "platform" + File.separator + "models" + File.separator + model + ".java"), template);
            created = true;
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return created;
    }

    private void init() throws DatabaseException {
        if (this.columns.isEmpty()) {
            String[] fields = Inspector.classFields(this);
            if (fields != null && fields.length > 0)
                for (String field : fields) {
                    Column meta = Inspector.columnMeta(this, field);
                    if (meta != null) {
                        if (!isTypeSupported(meta.type()))
                            throw new DatabaseException(meta.type() + " Is Not A Valid Type !");
                        LinkedHashMap<String, Object> column = new LinkedHashMap<>();
                        column.put("field", field);
                        column.put("name", meta.name());
                        column.put("autoincrement", Boolean.valueOf(meta.autoincrement()));
                        column.put("primary", Boolean.valueOf(meta.primary()));
                        column.put("type", meta.type());
                        column.put("nullable", Boolean.valueOf(meta.nullable()));
                        column.put("length", Integer.valueOf(meta.length()));
                        if (meta.primary())
                            this.primary = (LinkedHashMap)column;
                        this.columns.put(column.get("name").toString(), column);
                    }
                }
        }
        if (this.primary.isEmpty())
            throw new DatabaseException(this.table + this.primary + "This Active Record Class Must Have One Primary Column !");
    }

    private Object getFieldValue(String field) {
        Object value = null;
        try {
            value = getClass().getDeclaredField(field).get(this);
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
        return value;
    }

    private void setFieldValue(String field, Object value) {
        try {
            getClass().getDeclaredField(field).set(this, value);
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), ActiveRecord.class);
        }
    }

    private boolean isTypeSupported(String type) {
        return ("integer".equalsIgnoreCase(type) || "decimal".equalsIgnoreCase(type) || "text".equalsIgnoreCase(type) || "date".equalsIgnoreCase(type) || "timestamp".equalsIgnoreCase(type) || "boolean".equalsIgnoreCase(type));
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return this.schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return this.table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public LinkedHashMap<String, LinkedHashMap<String, Object>> getColumns() {
        return this.columns;
    }

    public void setColumns(LinkedHashMap<String, LinkedHashMap<String, Object>> columns) {
        this.columns = columns;
    }

    public LinkedHashMap<String, Object> getPrimary() {
        return this.primary;
    }

    public void setPrimary(LinkedHashMap<String, Object> primary) {
        this.primary = primary;
    }
}
