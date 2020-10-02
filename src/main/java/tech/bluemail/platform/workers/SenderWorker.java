package tech.bluemail.platform.workers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import tech.bluemail.platform.logging.Logger;
import tech.bluemail.platform.orm.Database;
import tech.bluemail.platform.parsers.TypesParser;
import tech.bluemail.platform.remote.SSH;
import tech.bluemail.platform.utils.Strings;

public class SenderWorker extends Thread {
    public int dropId;

    public SSH ssh;

    public File pickupFile;

    public File pickupsFolder;

    public SenderWorker(int dropId, SSH ssh, File pickupFile, File pickupsFolder) {
        this.dropId = dropId;
        this.ssh = ssh;
        this.pickupFile = pickupFile;
        this.pickupsFolder = pickupsFolder;
    }

    public void run() {
        try {
            if (this.ssh != null && this.pickupFile != null)
                if (this.pickupFile.exists()) {
                    int progress = TypesParser.safeParseInt(String.valueOf(this.pickupFile.getName().split("\\_")[2]));
                    String file = "/var/spool/bluemail/tmp/pickup_" + Strings.getSaltString(20, true, true, true, false) + ".txt";
                    this.ssh.uploadFile(this.pickupFile.getAbsolutePath(), file);
                    this.ssh.cmd("mv " + file + " /var/spool/bluemail/pickup/");
                    if (this.dropId > 0)
                        if (DropStatus().equalsIgnoreCase("interrupted")) {
                            if (this.pickupsFolder.exists())
                                deleteDirectoryStream(this.pickupsFolder.toPath());
                            if (this.pickupFile.exists())
                                deleteDirectoryStream(this.pickupFile.toPath());
                        } else {
                            ServerWorker.updateDrop(this.dropId, progress);
                        }
                }
        } catch (Exception e) {
            Logger.error(e, SenderWorker.class);
        }
    }

    public void deleteDirectoryStream(Path path) throws IOException {
        Files.walk(path, new java.nio.file.FileVisitOption[0])
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    public String DropStatus() {
        String status = "";
        try {
            List<LinkedHashMap<String, Object>> result = Database.get("master").executeQuery("SELECT status FROM production.drops WHERE id =" + this.dropId, null, 0);
            if (!result.isEmpty()) {
                status = (String)((LinkedHashMap)result.get(0)).get("status");
                return status;
            }
        } catch (Exception e) {
            Logger.error(e, ServerWorker.class);
        }
        return status;
    }
}
