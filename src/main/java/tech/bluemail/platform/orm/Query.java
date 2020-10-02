package tech.bluemail.platform.orm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import tech.bluemail.platform.exceptions.DatabaseException;
import tech.bluemail.platform.logging.Logger;

public class Query {
    private String database;

    private String from;

    private String[] fields;

    private int offset;

    private int limit;

    private String[] order;

    private String direction;

    private String[] group;

    private String[] join;

    private String[] where;

    private Object[] whereParameters;

    private Object[] parameters;

    private String query;

    public static final int SELECT = 0;

    public static final int INSERT = 1;

    public static final int UPDATE = 2;

    public static final int DELETE = 3;

    public static final int ONLY_BUILD_QUERY = 0;

    public static final int EXECUTE_QUERY = 1;

    public static final String ASC = "ASC";

    public static final String DESC = "DESC";

    public static final String LEFT_JOIN = "LEFT JOIN";

    public static final String RIGHT_JOIN = "RIGHT JOIN";

    public static final String INNER_JOIN = "INNER JOIN";

    public static final String FULL_OUTER_JOIN = "FULL OUTER JOIN";

    public List<LinkedHashMap<String, Object>> all() {
        List<LinkedHashMap<String, Object>> results = new ArrayList<>();
        try {
            if (this.whereParameters != null && this.whereParameters.length > 0)
                this.parameters = ArrayUtils.addAll(this.parameters, this.whereParameters);
            if ("".equalsIgnoreCase(this.query))
                build(0);
            results = Database.get(this.database).executeQuery(this.query, this.parameters, 1);
            reset();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), Query.class);
        }
        return results;
    }

    public LinkedHashMap<String, Object> first() {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        try {
            if (this.whereParameters != null && this.whereParameters.length > 0)
                this.parameters = ArrayUtils.addAll(this.parameters, this.whereParameters);
            if ("".equalsIgnoreCase(this.query))
                build(0);
            List<LinkedHashMap<String, Object>> results = Database.get(this.database).executeQuery(this.query, this.parameters, 0);
            row = !results.isEmpty() ? results.get(0) : row;
            reset();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), Query.class);
        }
        return row;
    }

    public int count() {
        int count = 0;
        try {
            if (this.whereParameters != null && this.whereParameters.length > 0)
                this.parameters = ArrayUtils.addAll(this.parameters, this.whereParameters);
            if ("".equalsIgnoreCase(this.query))
                build(0);
            count = Database.get(this.database).executeQuery(this.query, this.parameters, 1).size();
            reset();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), Query.class);
        }
        return count;
    }

    public int insert(Object[] parameters) {
        int result = 0;
        try {
            this.parameters = parameters;
            if ("".equalsIgnoreCase(this.query))
                build(1);
            result = Database.get(this.database).executeUpdate(this.query, this.parameters, 1);
            reset();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), Query.class);
        }
        return result;
    }

    public int update(Object[] parameters) {
        int result = 0;
        try {
            this.parameters = parameters;
            if (this.whereParameters != null && this.whereParameters.length > 0)
                this.parameters = ArrayUtils.addAll(this.parameters, this.whereParameters);
            if ("".equalsIgnoreCase(this.query))
                build(2);
            result = Database.get(this.database).executeUpdate(this.query, this.parameters, 0);
            reset();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), Query.class);
        }
        return result;
    }

    public int delete() {
        int result = 0;
        try {
            if (this.whereParameters != null && this.whereParameters.length > 0)
                this.parameters = this.whereParameters;
            if ("".equalsIgnoreCase(this.query))
                build(3);
            result = Database.get(this.database).executeUpdate(this.query, this.parameters, 0);
            reset();
        } catch (Exception e) {
            Logger.error((Exception)new DatabaseException(e), Query.class);
        }
        return result;
    }

    public Query from(String from, String[] fields) {
        this.from = from;
        if (fields == null || 0 == fields.length)
            fields = new String[] { "*" };
        this.fields = (String[])ArrayUtils.addAll((Object[])this.fields, (Object[])fields);
        return this;
    }

    public Query where(String condition, Object[] parameters, String concat) {
        concat = ("and".equalsIgnoreCase(concat) || "or".equalsIgnoreCase(concat) || "nand".equalsIgnoreCase(concat) || "nor".equalsIgnoreCase(concat)) ? (concat + " ") : "";
        this.where = (String[])ArrayUtils.add((Object[])this.where, concat + condition);
        this.whereParameters = ArrayUtils.addAll(this.whereParameters, parameters);
        return this;
    }

    public Query order(String[] columns, String direction) {
        this.order = (String[])ArrayUtils.addAll((Object[])this.order, (Object[])columns);
        this.direction = direction;
        return this;
    }

    public Query limit(int offset, int limit) {
        this.offset = offset;
        this.limit = limit;
        return this;
    }

    public Query group(String[] columns) {
        this.group = (String[])ArrayUtils.addAll((Object[])this.group, (Object[])columns);
        return this;
    }

    public Query join(String join, String on, String[] fields, String type) {
        type = (type == null || "".equalsIgnoreCase(type)) ? "LEFT JOIN" : type;
        if (fields == null)
            fields = new String[0];
        if (0 == fields.length)
            fields[0] = "*";
        this.fields = (String[])ArrayUtils.addAll((Object[])this.fields, (Object[])fields);
        this.join = (String[])ArrayUtils.add((Object[])this.join, type + " " + join + " ON " + on);
        return this;
    }

    public Query build(int type) throws DatabaseException {
        String template;
        String fields;
        String wheres;
        String str2;
        String values;
        String str1;
        String orders;
        int[] removeIndexes;
        String limit;
        int i;
        String joins;
        String groups;
        int j;
        switch (type) {
            case 0:
                template = "SELECT %s FROM %s %s %s %s %s %s";
                fields = "";
                str2 = "";
                orders = "";
                limit = "";
                joins = "";
                groups = "";
                for (j = 0; j < this.fields.length; j++) {
                    fields = fields + this.fields[j];
                    if (j != this.fields.length - 1)
                        fields = fields + ",";
                }
                if (this.join != null && this.join.length > 0)
                    for (j = 0; j < this.join.length; j++) {
                        joins = joins + this.join[j];
                        if (j != this.join.length - 1)
                            joins = joins + this.join[j] + " ";
                    }
                if (this.where != null && this.where.length > 0) {
                    str2 = "WHERE ";
                    for (j = 0; j < this.where.length; j++) {
                        str2 = str2 + this.where[j];
                        if (j != this.where.length - 1)
                            str2 = str2 + " ";
                    }
                }
                if (this.group != null && this.group.length > 0) {
                    groups = "GROUP BY ";
                    for (j = 0; j < this.group.length; j++) {
                        groups = groups + this.group[j];
                        if (j != this.group.length - 1)
                            groups = groups + ",";
                    }
                }
                if (this.order != null && this.order.length > 0) {
                    orders = "ORDER BY ";
                    for (j = 0; j < this.order.length; j++) {
                        orders = orders + this.order[j];
                        if (j != this.order.length - 1)
                            orders = orders + ",";
                    }
                    orders = orders + " " + this.direction;
                }
                if (this.limit > 0)
                    if (this.offset > 0) {
                        if ("mysql".equalsIgnoreCase(Database.get(this.database).getDriver())) {
                            limit = "LIMIT " + this.offset + "," + this.limit;
                        } else {
                            limit = "OFFSET " + this.offset + " LIMIT " + this.limit;
                        }
                    } else {
                        limit = "LIMIT " + this.limit;
                    }
                this.query = String.format(template, new Object[] { fields, this.from, joins, str2, groups, orders, limit });
                return this;
            case 1:
                template = "INSERT INTO %s (%s) VALUES (%s)";
                fields = "";
                values = "";
                removeIndexes = new int[0];
                for (i = 0; i < this.fields.length; i++) {
                    fields = fields + this.fields[i];
                    if (i != this.fields.length - 1)
                        fields = fields + ",";
                }
                for (i = 0; i < this.fields.length; i++) {
                    if (this.parameters[i] == null) {
                        values = values + "NULL";
                        removeIndexes = ArrayUtils.add(removeIndexes, i);
                    } else {
                        values = values + "?";
                    }
                    if (i != this.fields.length - 1)
                        values = values + ",";
                }
                for (int removeIndex : removeIndexes)
                    this.parameters = ArrayUtils.remove(this.parameters, removeIndex);
                this.query = String.format(template, new Object[] { this.from, fields, values });
                return this;
            case 2:
                template = "UPDATE %s SET %s %s";
                fields = "";
                str1 = "";
                removeIndexes = new int[0];
                for (i = 0; i < this.fields.length; i++) {
                    if (this.parameters[i] == null) {
                        fields = fields + this.fields[i] + " = NULL";
                        removeIndexes = ArrayUtils.add(removeIndexes, i);
                    } else {
                        fields = fields + this.fields[i] + " = ?";
                    }
                    if (i != this.fields.length - 1)
                        fields = fields + ",";
                }
                if (this.where != null && this.where.length > 0) {
                    str1 = "WHERE ";
                    for (i = 0; i < this.where.length; i++) {
                        str1 = str1 + this.where[i];
                        if (i != this.where.length - 1)
                            str1 = str1 + " ";
                    }
                }
                for (int removeIndex : removeIndexes)
                    this.parameters = ArrayUtils.remove(this.parameters, removeIndex);
                this.query = String.format(template, new Object[] { this.from, fields, str1 });
                return this;
            case 3:
                template = "DELETE FROM %s %s";
                wheres = "";
                if (this.where != null && this.where.length > 0) {
                    wheres = "WHERE ";
                    for (int k = 0; k < this.where.length; k++) {
                        wheres = wheres + this.where[k];
                        if (k != this.where.length - 1)
                            wheres = wheres + " ";
                    }
                }
                this.query = String.format(template, new Object[] { this.from, wheres });
                return this;
        }
        throw new DatabaseException("Unsupported query type !");
    }

    private void reset() {
        this.from = "";
        this.fields = new String[0];
        this.offset = 0;
        this.limit = 0;
        this.order = new String[0];
        this.direction = "ASC";
        this.group = new String[0];
        this.join = new String[0];
        this.where = new String[0];
        this.parameters = new Object[0];
        this.query = "";
    }

    public Query(String database) {
        this.database = Database.getDefault().getKey();
        this.from = "";
        this.fields = new String[0];
        this.offset = 0;
        this.limit = 0;
        this.order = new String[0];
        this.direction = "ASC";
        this.group = new String[0];
        this.join = new String[0];
        this.where = new String[0];
        this.whereParameters = new Object[0];
        this.parameters = new Object[0];
        this.query = "";
        this.database = database;
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public Object[] getWhereParameters() {
        return this.whereParameters;
    }

    public void setWhereParameters(Object[] whereParameters) {
        this.whereParameters = whereParameters;
    }

    public String getFrom() {
        return this.from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String[] getFields() {
        return this.fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public int getOffset() {
        return this.offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLimit() {
        return this.limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String[] getOrder() {
        return this.order;
    }

    public void setOrder(String[] order) {
        this.order = order;
    }

    public String getDirection() {
        return this.direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String[] getGroup() {
        return this.group;
    }

    public void setGroup(String[] group) {
        this.group = group;
    }

    public String[] getJoin() {
        return this.join;
    }

    public void setJoin(String[] join) {
        this.join = join;
    }

    public String[] getWhere() {
        return this.where;
    }

    public void setWhere(String[] where) {
        this.where = where;
    }

    public Object[] getParameters() {
        return this.parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
