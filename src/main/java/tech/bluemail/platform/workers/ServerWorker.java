package tech.bluemail.platform.workers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import tech.bluemail.platform.components.DropComponent;
import tech.bluemail.platform.components.RotatorComponent;
import tech.bluemail.platform.exceptions.DatabaseException;
import tech.bluemail.platform.exceptions.ThreadException;
import tech.bluemail.platform.helpers.DropsHelper;
import tech.bluemail.platform.logging.Logger;
import tech.bluemail.platform.models.admin.Server;
import tech.bluemail.platform.models.admin.Vmta;
import tech.bluemail.platform.orm.Database;
import tech.bluemail.platform.parsers.TypesParser;
import tech.bluemail.platform.remote.SSH;
import tech.bluemail.platform.utils.Strings;

public class ServerWorker extends Thread {
    public DropComponent drop;

    public Server server;

    public List<Vmta> vmtas;

    public int offset;

    public int limit;

    public String query;

    public List<PickupWorker> pickupWorkers = new ArrayList<>();

    public List<SenderWorker> sendersWorkers = new ArrayList<>();

    public ServerWorker(DropComponent drop, Server server, List<Vmta> vmtas, int offset, int limit) {
        this.drop = drop;
        this.server = server;
        this.vmtas = vmtas;
        this.offset = offset;
        this.limit = limit;
        if (this.drop.isSend) {
            this.query = "SELECT * FROM (";
            this.drop.lists.entrySet().stream().map(en -> {
                this.query += "SELECT id,'" + String.valueOf(en.getValue()) + "' AS table,'" + String.valueOf(en.getKey()) + "' AS list_id,fname,lname,email";
                return en;
            }).map(en -> {
                this.query += String.valueOf(en.getValue()).contains("seeds") ? (",generate_series(1," + drop.emailsPerSeeds + ") AS serie") : ",id AS serie";
                return en;
            }).forEachOrdered(en -> this.query += " FROM " + String.valueOf(en.getValue()) + " UNION ALL ");
            this.query = this.query.substring(0, this.query.length() - 10) + " WHERE (offers_excluded IS NULL OR offers_excluded = '' OR NOT ('" + this.drop.offerId + "' = ANY(string_to_array(offers_excluded,',')))) ORDER BY id OFFSET " + this.drop.dataStart + " LIMIT " + this.drop.dataCount + ") As Sub OFFSET " + this.offset + " LIMIT " + this.limit;
        }
        if (!this.vmtas.isEmpty()) {
            int rotation = this.drop.isSend ? this.drop.vmtasRotation : this.drop.testEmails.length;
            this.drop.vmtasRotator = new RotatorComponent(this.vmtas, rotation);
        }
        this.drop.pickupsFolder = System.getProperty("base.path") + "/tmp/pickups/server_" + this.server.id + "_" + Strings.getSaltString(20, true, true, true, false);
        (new File(this.drop.pickupsFolder)).mkdirs();
    }

    public void run() {
        SSH ssh = null;
        boolean errorOccured = false;
        boolean isStopped = false;
        try {
            if (this.server != null && this.server.id > 0 && !this.vmtas.isEmpty()) {
                if (this.server.server_auth != null && !"".equalsIgnoreCase(this.server.server_auth) && Integer.parseInt(this.server.server_auth) == 1) {
                    ssh = SSH.SSHKey(this.server.mainIp, this.server.username, String.valueOf(this.server.sshPort), "/home/keys/id_rsa");
                } else {
                    ssh = SSH.SSHPassword(this.server.mainIp, String.valueOf(this.server.sshPort), this.server.username, this.server.password);
                }
                ssh.connect();
                if (this.drop.uploadImages)
                    DropsHelper.uploadImage(this.drop, ssh);
                if (this.vmtas.isEmpty())
                    throw new Exception("No Vmtas Found !");
                List<LinkedHashMap<String, Object>> result = null;
                if (this.drop.isSend) {
                    result = Database.get("lists").executeQuery(this.query, null, 1);
                    this.drop.emailsCount = result.size();
                } else {
                    result = new ArrayList<>();
                    if (this.drop.testEmails != null && this.drop.testEmails.length > 0)
                        for (Vmta vmta : this.vmtas) {
                            if (vmta != null)
                                for (String testEmail : this.drop.testEmails) {
                                    LinkedHashMap<String, Object> tmp = new LinkedHashMap<>();
                                    tmp.put("id", Integer.valueOf(0));
                                    tmp.put("email", testEmail.trim());
                                    tmp.put("table", "");
                                    tmp.put("list_id", Integer.valueOf(0));
                                    result.add(tmp);
                                }
                        }
                }
                if (this.drop.isSend && this.drop.isNewDrop) {
                    DropsHelper.saveDrop(this.drop, this.server);
                    if (this.drop.id > 0) {
                        if (this.vmtas.isEmpty())
                            throw new Exception("No Vmtas Found !");
                        int vmtasTotal = (int)Math.ceil((this.drop.emailsCount / this.vmtas.size()));
                        int vmtasRest = this.drop.emailsCount - vmtasTotal * this.vmtas.size();
                        int index = 0;
                        if (!this.vmtas.isEmpty())
                            for (Vmta vmta : this.vmtas) {
                                if (index < vmtasRest) {
                                    DropsHelper.saveDropVmta(this.drop, vmta, vmtasTotal + 1);
                                } else {
                                    DropsHelper.saveDropVmta(this.drop, vmta, vmtasTotal);
                                }
                                index++;
                            }
                    }
                }
                if (this.drop.isSend)
                    DropsHelper.writeThreadStatusFile(this.server.id, this.drop.pickupsFolder);
                if (!this.drop.isSend)
                    this.drop.emailsCount = this.drop.testEmails.length * this.vmtas.size();
                this.drop.batch = (this.drop.batch > this.drop.emailsCount) ? this.drop.emailsCount : this.drop.batch;
                this.drop.batch = (this.drop.batch == 0) ? 1 : this.drop.batch;
                ExecutorService pickupsExecutor = Executors.newFixedThreadPool(100);
                if (this.drop.batch == 0)
                    throw new Exception("Batch should be greather than 0 !");
                int pickupsNumber = (this.drop.emailsCount % this.drop.batch == 0) ? (int)Math.ceil((this.drop.emailsCount / this.drop.batch)) : ((int)Math.ceil((this.drop.emailsCount / this.drop.batch)) + 1);
                int start = 0;
                int finish = this.drop.batch;
                Vmta periodVmta = null;
                PickupWorker worker = null;
                for (int i = 0; i < pickupsNumber; i++) {
                    if (this.drop != null && this.drop.isSend && this.drop.id > 0) {
                        String status = DropStatus();
                        if (DropsHelper.hasToStopDrop(this.server.id, this.drop.pickupsFolder) || status == "interrupted") {
                            interrupt();
                            pickupsExecutor.shutdownNow();
                            interruptDrop();
                            isStopped = true;
                            this.drop.isStoped = true;
                            break;
                        }
                    }
                    if (!isStopped && !isInterrupted() && !this.drop.isStoped) {
                        periodVmta = "emails-per-period".equalsIgnoreCase(this.drop.vmtasEmailsProcces) ? this.drop.getCurrentVmta() : null;
                        worker = new PickupWorker(i, this.drop, this.server, result.subList(start, finish), periodVmta);
                        worker.setUncaughtExceptionHandler((Thread.UncaughtExceptionHandler)new ThreadException());
                        pickupsExecutor.submit(worker);
                        this.pickupWorkers.add(worker);
                        start += this.drop.batch;
                        finish += this.drop.batch;
                        if (finish > result.size())
                            finish = result.size();
                        if (start >= result.size())
                            break;
                    } else {
                        pickupsExecutor.shutdownNow();
                        interrupt();
                        this.pickupWorkers.forEach(previousWorker -> {
                            if (previousWorker.isAlive())
                                previousWorker.interrupt();
                        });
                        deleteDirectoryStream((new File(this.drop.pickupsFolder)).toPath());
                        break;
                    }
                }
                pickupsExecutor.shutdown();
                pickupsExecutor.awaitTermination(1L, TimeUnit.DAYS);
                if (!isStopped && !this.drop.isStoped) {
                    File[] pickupsFiles = (new File(this.drop.pickupsFolder)).listFiles();
                    if (pickupsFiles != null && pickupsFiles.length > 0)
                        if (ssh.isConnected() && !this.drop.isStoped) {
                            File[] tmp = this.drop.isSend ? new File[pickupsFiles.length - 1] : new File[pickupsFiles.length];
                            int idx = 0;
                            for (File pickupsFile : pickupsFiles) {
                                if (pickupsFile.getName().startsWith("pickup_")) {
                                    tmp[idx] = pickupsFile;
                                    idx++;
                                }
                            }
                            pickupsFiles = tmp;
                            Arrays.sort(pickupsFiles, (f1, f2) -> (new Integer(TypesParser.safeParseInt(((File)f1).getName().split("_")[1]))).compareTo(Integer.valueOf(TypesParser.safeParseInt(((File)f2).getName().split("_")[1]))));
                            ExecutorService senderExecutor = Executors.newFixedThreadPool(100);
                            SenderWorker senderWorker = null;
                            for (File pickupsFile : pickupsFiles) {
                                if (this.drop != null && this.drop.isSend && this.drop.id > 0) {
                                    String status = DropStatus();
                                    if (DropsHelper.hasToStopDrop(this.server.id, this.drop.pickupsFolder) || status == "interrupted") {
                                        senderExecutor.shutdownNow();
                                        interruptDrop();
                                        isStopped = true;
                                        break;
                                    }
                                }
                                if (!isStopped && !this.drop.isStoped) {
                                    senderWorker = new SenderWorker(this.drop.id, ssh, pickupsFile, new File(this.drop.pickupsFolder));
                                    senderWorker.setUncaughtExceptionHandler((Thread.UncaughtExceptionHandler)new ThreadException());
                                    senderExecutor.submit(senderWorker);
                                    this.sendersWorkers.add(senderWorker);
                                    if (this.drop.delay > 0L)
                                        Thread.sleep(this.drop.delay);
                                } else {
                                    senderExecutor.shutdownNow();
                                    interrupt();
                                    this.sendersWorkers.forEach(previousWorker -> {
                                        if (previousWorker.isAlive())
                                            previousWorker.interrupt();
                                    });
                                    break;
                                }
                            }
                            senderExecutor.shutdown();
                            senderExecutor.awaitTermination(1L, TimeUnit.DAYS);
                        }
                } else {
                    pickupsExecutor.shutdownNow();
                    interrupt();
                    this.pickupWorkers.forEach(previousWorker -> {
                        if (previousWorker.isAlive())
                            previousWorker.interrupt();
                    });
                    deleteDirectoryStream((new File(this.drop.pickupsFolder)).toPath());
                }
            }
        } catch (Exception e) {
            if (this.drop != null && this.drop.isSend && this.drop.id > 0)
                errorDrop();
            Logger.error(e, ServerWorker.class);
            errorOccured = true;
        } finally {
            finishProccess(ssh, errorOccured, isStopped);
        }
    }

    public static synchronized void updateDrop(int dropId, int progress) throws DatabaseException {
        Database.get("master").executeUpdate("UPDATE production.drops SET sent_progress = sent_progress + '" + progress + "'  WHERE id = ?", new Object[] { Integer.valueOf(dropId) }, 0);
    }

    public void finishProccess(SSH ssh, boolean errorOccured, boolean isStopped) {
        try {
            if (ssh != null && ssh.isConnected())
                ssh.disconnect();
            if (this.drop != null) {
                if (this.drop.id > 0 && !errorOccured && !isStopped) {
                    int progress = 0;
                    List<LinkedHashMap<String, Object>> result = Database.get("master").executeQuery("SELECT sent_progress FROM production.drops WHERE id =" + this.drop.id, null, 0);
                    if (!result.isEmpty()) {
                        progress = ((Integer)((LinkedHashMap)result.get(0)).get("sent_progress")).intValue();
                        if (progress == this.drop.emailsCount) {
                            Database.get("master").executeUpdate("UPDATE production.drops SET status = 'completed' , finish_time = ?  WHERE id = ?", new Object[] { new Timestamp(System.currentTimeMillis()), Integer.valueOf(this.drop.id) }, 0);
                        } else {
                            System.out.println("NOT COUNT");
                        }
                    }
                }
                FileUtils.deleteDirectory(new File(this.drop.pickupsFolder));
            }
        } catch (Exception e) {
            Logger.error(e, ServerWorker.class);
        }
    }

    public void errorDrop() {
        try {
            Database.get("master").executeUpdate("UPDATE production.drops SET status = 'error' , finish_time = ?  WHERE id = ?", new Object[] { new Timestamp(System.currentTimeMillis()), Integer.valueOf(this.drop.id) }, 0);
            if (this.drop != null)
                FileUtils.deleteDirectory(new File(this.drop.pickupsFolder));
        } catch (Exception e) {
            Logger.error(e, ServerWorker.class);
        }
    }

    public void interruptDrop() {
        try {
            Database.get("master").executeUpdate("UPDATE production.drops SET status = 'interrupted' , finish_time = ?  WHERE id = ?", new Object[] { new Timestamp(System.currentTimeMillis()), Integer.valueOf(this.drop.id) }, 0);
            if (this.drop != null)
                FileUtils.deleteDirectory(new File(this.drop.pickupsFolder));
        } catch (Exception e) {
            Logger.error(e, ServerWorker.class);
        }
    }

    public String DropStatus() {
        String status = "";
        try {
            List<LinkedHashMap<String, Object>> result = Database.get("master").executeQuery("SELECT status FROM production.drops WHERE id =" + this.drop.id, null, 0);
            if (!result.isEmpty()) {
                status = (String)((LinkedHashMap)result.get(0)).get("status");
                return status;
            }
        } catch (Exception e) {
            Logger.error(e, ServerWorker.class);
        }
        return status;
    }

    public void deleteDirectoryStream(Path path) throws IOException {
        Files.walk(path, new java.nio.file.FileVisitOption[0])
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}
