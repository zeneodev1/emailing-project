package tech.bluemail.platform.workers;

import org.apache.commons.io.FileUtils;
import tech.bluemail.platform.controllers.SuppressionManager;
import tech.bluemail.platform.exceptions.DatabaseException;
import tech.bluemail.platform.logging.Logger;
import tech.bluemail.platform.models.admin.DataList;
import tech.bluemail.platform.orm.Connector;
import tech.bluemail.platform.orm.Database;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;



public class SupressionWorker extends Thread {
    public int proccessId;

    public int offerId;

    public DataList dataList;

    public boolean isMd5;

    public String directory;

    public int listsSize;

    public SupressionWorker(int proccessId, int offerId, DataList dataList, boolean isMd5, String directory, int listsSize) {
        this.proccessId = proccessId;
        this.offerId = offerId;
        this.dataList = dataList;
        this.isMd5 = isMd5;
        this.directory = directory;
        this.listsSize = listsSize;
    }

    public void run() {
        try {
            if (this.dataList != null && this.proccessId > 0 && this.offerId > 0) {
                List<String> suppressionEmails = new ArrayList<>();
                String[] columns = null;
                String schema = this.dataList.name.split("\\.")[0];
                String table = this.dataList.name.split("\\.")[1];
                System.out.println("Table -> " + table);
                if (table.startsWith("fresh_") || table.startsWith("clean_")) {
                    columns = new String[]{"id", "email", "fname", "lname", "offers_excluded"};
                } else if (table.startsWith("unsubscribers_")) {
                    columns = new String[]{"id", "email", "fname", "lname", "drop_id", "action_date", "message", "offers_excluded", "verticals", "agent", "ip", "country", "region", "city", "language", "device_type", "device_name", "os", "browser_name", "browser_version"};
                } else {
                    columns = new String[]{"id", "email", "fname", "lname", "action_date", "offers_excluded", "verticals", "agent", "ip", "country", "region", "city", "language", "device_type", "device_name", "os", "browser_name", "browser_version"};
                }
                // initData(columns, types);
                List<LinkedHashMap<String, Object>> totalEmails = getsuppressionEmails(suppressionEmails, columns);

                if (!suppressionEmails.isEmpty() && !totalEmails.isEmpty()) {

                    System.out.println("table is not empty");

                    Collections.sort(suppressionEmails);
                    suppressionEmails.retainAll(SuppressionManager.MD5_EMAILS);
                    Set<String> setSupp = new HashSet<>(suppressionEmails);

                    List<LinkedHashMap<String, Object>> newList = applyChanges(totalEmails, setSupp, columns);

                    batchUpdate(dataList.name, newList);
                }
            }
        } catch (Exception e) {
            Logger.error(e, SupressionWorker.class);
        }
    }

    public List<LinkedHashMap<String, Object>> getsuppressionEmails(List<String> suppressionEmails, String[] columns) {
        List<LinkedHashMap<String, Object>> emails = null;
        try {
            if (this.isMd5) {
                emails = Database.get("lists").executeQuery("SELECT " + String.join(",", columns) + ",md5(email) as md5_email FROM " + this.dataList.name, null, 1);
            } else {
                emails = Database.get("lists").executeQuery("SELECT " + String.join(",", columns) + ",email as md5_email FROM " + this.dataList.name, null, 1);
            }
            for (LinkedHashMap<String, Object> row : emails) {
                if (row != null)
                    suppressionEmails.add(String.valueOf(row.get("md5_email")).trim());
            }
        } catch (Exception e) {
            Logger.error(e, SupressionWorker.class);
        }
        return emails;
    }

    public List<LinkedHashMap<String, Object>> applyChanges(List<LinkedHashMap<String, Object>> totalEmails, Set<String> suppressionEmails, String[] columns) throws SQLException {
        boolean insertOfferId = false;
        List<String> offerIds = null;
        for (int i = 0; i < totalEmails.size(); i++) {
            LinkedHashMap<String, Object> row = totalEmails.get(i);
            insertOfferId = suppressionEmails.contains(String.valueOf(row.get("md5_email")).trim());
            if (row.get("offers_excluded") == null || "null".equalsIgnoreCase(String.valueOf(row.get("offers_excluded"))) || "".equalsIgnoreCase(String.valueOf(row.get("offers_excluded")))) {
                if (insertOfferId == true) {
                    totalEmails.get(i).replace("offers_excluded", offerId);
                }
            } else {
                if (insertOfferId == true) {
                    offerIds = new ArrayList<String>(new HashSet<>(Arrays.asList(String.valueOf(row.get("offers_excluded")).split(","))));
                    if (!offerIds.contains(String.valueOf(offerId)))
                        totalEmails.get(i).replace("offers_excluded", offerId + "," + totalEmails.get(i).get("offers_excluded"));
                }
            }

        }
        return totalEmails;
    }

    public void batchUpdate(String table, List<LinkedHashMap<String, Object>> entities) throws SQLException {
        Connection connection = Database.get("lists").getDataSource().getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("update " + table + " SET offers_excluded=? WHERE id=?");

            int i = 0;
            for (LinkedHashMap<String, Object> entity : entities) {
                i++;
                preparedStatement.setString(1, String.valueOf(entity.get("offers_excluded")));

                preparedStatement.setInt(2, Integer.parseInt(String.valueOf(entity.get("id"))));
                preparedStatement.addBatch();

                if (i == 5000 || i == entities.size()) {
                    preparedStatement.executeBatch();
                }
            }

            preparedStatement.close();
        } catch (SQLException e) {
            Logger.error(e, Connector.class);
        } finally {
            connection.close();
        }

    }



    public void initData(String[] columns, String[] types) throws IOException, DatabaseException, SQLException {
        List<LinkedHashMap<String, Object>> totalEmails = new ArrayList<>();

        List<String> md5Emails = new ArrayList<>(SuppressionManager.MD5_EMAILS);
        for (int i = 0; i < md5Emails.size(); i++) {
            String s = md5Emails.get(i);
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            map.put("id", String.valueOf(2000 + i));
            map.put("email", s);
            map.put("fname", "gg");
            map.put("lname", "gg");
            map.put("offers_excluded", "18");
            map.put("action_date", null);
            totalEmails.add(map);
        }

    }


}
