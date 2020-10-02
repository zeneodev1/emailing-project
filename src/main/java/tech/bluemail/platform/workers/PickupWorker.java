package tech.bluemail.platform.workers;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.net.QuotedPrintableCodec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import tech.bluemail.platform.components.DropComponent;
import tech.bluemail.platform.controllers.DropsSender;
import tech.bluemail.platform.exceptions.DatabaseException;
import tech.bluemail.platform.helpers.DropsHelper;
import tech.bluemail.platform.logging.Logger;
import tech.bluemail.platform.models.admin.Server;
import tech.bluemail.platform.models.admin.Vmta;
import tech.bluemail.platform.models.lists.Fresh;
import tech.bluemail.platform.parsers.TypesParser;
import tech.bluemail.platform.security.Crypto;
import tech.bluemail.platform.utils.Domains;
import tech.bluemail.platform.utils.Strings;

public class PickupWorker extends Thread {
    public int index;

    public DropComponent drop;

    public Server server;

    public List<LinkedHashMap<String, Object>> emails;

    public Vmta defaultVmta;

    public PickupWorker(int index, DropComponent drop, Server server, List<LinkedHashMap<String, Object>> emails, Vmta defaultVmta) {
        this.index = index;
        this.drop = drop;
        this.server = server;
        this.emails = emails;
        this.defaultVmta = defaultVmta;
    }

    public void run() {
        try {
            StringBuilder pickup = new StringBuilder();
            int globalCounter = 0;
            int pickupTotal = 0;
            if (this.emails != null && !this.emails.isEmpty() && !this.drop.isStoped) {
                StringBuilder messageIdBuilder = new StringBuilder();
                Fresh email = createEmailObject(this.emails.get(0));
                Vmta vmta = (this.defaultVmta != null) ? this.defaultVmta : (Vmta) this.drop.vmtasRotator.getList().get(0);
                String messageId = "<" + messageIdBuilder.append(messageIdBuilder.hashCode()).append('.').append(Strings.getSaltString(13, false, false, true, false)).append('.').append(System.currentTimeMillis()).append('@').append(vmta.domain).toString() + ">";
                String mailDate = (new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z")).format(new Date());
                String globalBounceEmail = replaceTags(this.drop.bounceEmail, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId);
                pickup.append("XACK ON \n");
                pickup.append("XMRG FROM: <");
                pickup.append(globalBounceEmail);
                System.out.println("BOUNCE EMAIL --> " + globalBounceEmail);
                pickup.append(">\n");
                for (LinkedHashMap<String, Object> row : this.emails) {
                    email = createEmailObject(row);
                    vmta = (this.defaultVmta != null) ? this.defaultVmta : this.drop.getCurrentVmta();
                    createMailMerge(email, vmta, pickup);
                    globalCounter = this.drop.updateCounter();
                    if (this.drop.isSend && globalCounter > 0 && globalCounter % this.drop.testFrequency == 0)
                        if (this.drop.testEmails != null && this.drop.testEmails.length > 0)
                            for (String testEmail : this.drop.testEmails) {
                                email = new Fresh();
                                email.setSchema("");
                                email.setTable("");
                                email.email = testEmail.trim();
                                email.fname = testEmail.trim().split("\\@")[0];
                                email.lname = testEmail.trim().split("\\@")[0];
                                createMailMerge(email, vmta, pickup);
                            }
                    DropsSender.rotatePlaceHolders();
                    pickupTotal++;
                }
                String header = new String(String.valueOf(DropsSender.getCurrentHeader()).getBytes());
                String body = new String(this.drop.body.getBytes());
                pickup.append("XPRT 1 LAST \n");
                pickup.append(header);
                pickup.append("\n");
                if (this.drop.isSend && this.drop.trackOpens) {
                    pickup.append("<img alt='' src='http://[domain]/[open]' width='1px' height='1px' style='visibility:hidden'/>");
                    pickup.append("\n");
                }
                if (!"".equals(body))
                    switch (this.drop.contentTransferEncoding) {
                        case "Quoted-Printable":
                            body = (new QuotedPrintableCodec()).encode(body);
                            break;
                        case "base64":
                            body = new String(Base64.encodeBase64(body.getBytes()));
                            break;
                    }
                pickup.append("\n");
                String negative = "";
                pickup.append(body);
                pickup.append("\n");
                if (!"".equalsIgnoreCase(this.drop.negativeFileName) && this.drop.negativeFileName != null) {
                    String Path = new String(Base64.decodeBase64(this.drop.negativeFileName.getBytes()));
                    negative = new String(Files.readAllBytes(Paths.get(Path, new String[0])));
                    pickup.append(negative);
                }
                pickup.append("\n.\n");
                DropsSender.rotateHeaders();
                FileUtils.writeStringToFile(new File(this.drop.pickupsFolder + File.separator + "pickup_" + this.index + "_" + pickupTotal + "_" + Strings.getSaltString(8, true, true, true, false) + ".txt"), pickup.toString());
            }
        } catch (Exception e) {
            Logger.error(e, PickupWorker.class);
        }
    }

    public synchronized void createMailMerge(Fresh email, Vmta vmta, StringBuilder pickup) throws Exception {
        if (email != null && !"".equals(email.email.trim()))
            if (vmta != null) {
                String url = "r.php?t=c&d=" + this.drop.id + "&l=" + email.listId + "&c=" + email.id;
                String unsub = "r.php?t=u&d=" + this.drop.id + "&l=" + email.listId + "&c=" + email.id;
                String optout = "opt.php?d=" + this.drop.id + "&l=" + email.listId + "&c=" + email.id + "&em=" + Crypto.md5(email.email);
                if (!this.drop.isSend || email.getTable().contains("seeds")) {
                    url = "r.php?t=c&d=0&l=0&c=0&cr=" + this.drop.creativeId;
                    unsub = "r.php?t=u&d=0&l=0&c=0&cr=" + this.drop.creativeId;
                    optout = "opt.php?d=0&l=0&c=0&em=" + Crypto.md5(email.email);
                }
                if ("Quoted-Printable".equalsIgnoreCase(this.drop.contentTransferEncoding)) {
                    url = (new QuotedPrintableCodec()).encode(url);
                    unsub = (new QuotedPrintableCodec()).encode(unsub);
                    optout = (new QuotedPrintableCodec()).encode(optout);
                }
                StringBuilder messageIdBuilder = new StringBuilder();
                String messageId = "<" + messageIdBuilder.append(messageIdBuilder.hashCode()).append('.').append(Strings.getSaltString(13, false, false, true, false)).append('.').append(System.currentTimeMillis()).append('@').append(vmta.domain).toString() + ">";
                String mailDate = (new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z")).format(new Date());
                String bounceEmail = replaceTags(this.drop.bounceEmail, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId);
                String returnPath = replaceTags(this.drop.returnPath, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId);
                String fromEmail = replaceTags(this.drop.fromEmail, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId);
                String replyTo = replaceTags(this.drop.replyTo, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId);
                String to = replaceTags(this.drop.to, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId);
                String received = StringUtils.replace(replaceTags(this.drop.received, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId), "[return_path]", returnPath);
                String fromName = replaceTags(this.drop.fromName, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId);
                String subject = replaceTags(this.drop.subject, vmta, email, mailDate, messageId, this.drop.staticDomain, this.drop.id, this.drop.mailerId);
                String autoResponseEmail = "";
                pickup.append("XDFN ");
                pickup.append("rcpt=\"");
                if (this.drop.isSend && this.drop.isAutoResponse && this.drop.autoReplyEmails.length > 0) {
                    pickup.append(DropsSender.getCurrentAutoReplay().trim());
                    DropsSender.rotateAutoReplay();
                } else {
                    pickup.append(email.email);
                }
                pickup.append("\" ");
                pickup.append("mail_date=\"");
                pickup.append(mailDate);
                pickup.append("\" ");
                pickup.append("message_id=\"");
                pickup.append(messageId);
                pickup.append("\" ");
                pickup.append("ip=\"");
                pickup.append(vmta.ipValue);
                pickup.append("\" ");
                pickup.append("vmta_name=\"");
                pickup.append(vmta.name);
                pickup.append("\" ");
                pickup.append("smtphost=\"");
                pickup.append(vmta.smtphost);
                pickup.append("\" ");
                pickup.append("username=\"");
                pickup.append(vmta.username);
                pickup.append("\" ");
                pickup.append("password=\"");
                pickup.append(vmta.password);
                pickup.append("\" ");
                if (this.drop.staticDomain != null && !"".equalsIgnoreCase(this.drop.staticDomain)) {
                    pickup.append("rdns=\"");
                    pickup.append(this.drop.staticDomain);
                    pickup.append("\" ");
                    pickup.append("domain=\"");
                    pickup.append(this.drop.staticDomain);
                    pickup.append("\" ");
                } else {
                    pickup.append("rdns=\"");
                    pickup.append(vmta.domain);
                    pickup.append("\" ");
                    pickup.append("domain=\"");
                    pickup.append(Domains.getDomainName(vmta.domain));
                    pickup.append("\" ");
                }
                pickup.append("server=\"");
                pickup.append(this.server.name);
                pickup.append("\" ");
                pickup.append("mailer_id=\"");
                pickup.append(this.drop.mailerId);
                pickup.append("\" ");
                pickup.append("drop_id=\"");
                pickup.append(this.drop.id);
                pickup.append("\" ");
                pickup.append("list_id=\"");
                pickup.append(email.listId);
                pickup.append("\" ");
                pickup.append("email_id=\"");
                pickup.append(email.id);
                pickup.append("\" ");
                pickup.append("email=\"");
                pickup.append(email.email);
                pickup.append("\" ");
                pickup.append("auto_reply_email=\"");
                pickup.append(autoResponseEmail);
                pickup.append("\" ");
                pickup.append("fname=\"");
                pickup.append(email.fname);
                pickup.append("\" ");
                pickup.append("lname=\"");
                pickup.append(email.lname);
                pickup.append("\" ");
                pickup.append("email_name=\"");
                pickup.append(email.email.split("\\@")[0]);
                pickup.append("\" ");
                pickup.append("from_email=\"");
                pickup.append(fromEmail);
                pickup.append("\" ");
                pickup.append("return_path=\"");
                pickup.append(returnPath);
                pickup.append("\" ");
                pickup.append("reply_to=\"");
                pickup.append(replyTo);
                pickup.append("\" ");
                pickup.append("to=\"");
                pickup.append(to);
                pickup.append("\" ");
                pickup.append("received=\"");
                pickup.append(received);
                pickup.append("\" ");
                pickup.append("from_name=\"");
                pickup.append(fromName);
                pickup.append("\" ");
                pickup.append("subject=\"");
                pickup.append(subject);
                pickup.append("\" ");
                pickup.append("content_transfer_encoding=\"");
                pickup.append(this.drop.contentTransferEncoding);
                pickup.append("\" ");
                pickup.append("content_type=\"");
                pickup.append(this.drop.contentType);
                pickup.append("\" ");
                pickup.append("charset=\"");
                pickup.append(this.drop.charset);
                pickup.append("\" ");
                String openUrl = "r.php?t=o&d=" + this.drop.id + "&l=" + email.listId + "&c=" + email.id;
                if ("Quoted-Printable".equalsIgnoreCase(this.drop.contentTransferEncoding))
                    openUrl = (new QuotedPrintableCodec()).encode(openUrl);
                pickup.append("open=\"");
                pickup.append(openUrl);
                pickup.append("\" ");
                pickup.append("url=\"");
                pickup.append(url);
                pickup.append("\" ");
                pickup.append("unsub=\"");
                pickup.append(unsub);
                pickup.append("\" ");
                pickup.append("optout=\"");
                pickup.append(optout);
                pickup.append("\" ");
                if (this.drop.hasPlaceholders) {
                    pickup.append("placeholder=\"");
                    pickup.append(DropsSender.getCurrentPlaceHolder());
                    pickup.append("\" ");
                }
                if (this.drop.randomTags != null && this.drop.randomTags.length > 0)
                    for (String randomTag : this.drop.randomTags) {
                        pickup.append(randomTag).append("=\"");
                        pickup.append(DropsHelper.replaceRandomTag(randomTag));
                        pickup.append("\" ");
                    }
                pickup.append("\n");
                pickup.append("XDFN *vmta=\"");
                pickup.append(vmta.name);
                pickup.append("\" *jobId=\"");
                pickup.append(this.drop.mailerId);
                pickup.append("\" *from=\"");
                pickup.append(bounceEmail);
                pickup.append("\" *envId=\"");
                pickup.append(this.drop.id);
                pickup.append("_");
                pickup.append(vmta.ipId);
                pickup.append("_");
                pickup.append(email.id);
                pickup.append("_");
                pickup.append(email.listId);
                pickup.append("\"\n");
                pickup.append("RCPT TO:<");
                if (!"".equalsIgnoreCase(this.drop.rcptfrom)) {
                    pickup.append(this.drop.rcptfrom);
                } else if (this.drop.isSend && this.drop.isAutoResponse && this.drop.autoReplyEmails.length > 0) {
                    pickup.append(DropsSender.getCurrentAutoReplay().trim());
                    DropsSender.rotateAutoReplay();
                } else {
                    pickup.append(email.email);
                }
                pickup.append(">");
                pickup.append("\n");
            }
    }

    public Fresh createEmailObject(LinkedHashMap<String, Object> row) throws DatabaseException {
        Fresh email = new Fresh();
        if (!"".equalsIgnoreCase(String.valueOf(row.get("table")))) {
            email.setSchema(String.valueOf(row.get("table")).split("\\.")[0]);
            email.setTable(String.valueOf(row.get("table")).split("\\.")[1]);
        } else {
            email.setSchema("");
            email.setTable("");
        }
        email.map(row);
        email.fname = (email.fname == null || "".equals(email.fname) || "null".equalsIgnoreCase(email.fname)) ? email.email.split("\\@")[0] : email.fname;
        email.lname = (email.lname == null || "".equals(email.lname) || "null".equalsIgnoreCase(email.lname)) ? email.fname : email.lname;
        email.listId = TypesParser.safeParseInt(String.valueOf(row.get("list_id")));
        return email;
    }

    public String replaceTags(String value, Vmta vmta, Fresh email, String mailDate, String messageId, String staticDomain, int dropId, int mailerId) {
        String val = value;
        if (value != null && !"".equals(value)) {
            val = StringUtils.replace(value, "[drop_id]", String.valueOf(dropId));
            val = StringUtils.replace(value, "[mailer_id]", String.valueOf(mailerId));
            val = StringUtils.replace(value, "[ip]", vmta.ipValue);
            val = StringUtils.replace(val, "[smtphost]", vmta.smtphost);
            val = StringUtils.replace(val, "[username]", vmta.username);
            val = StringUtils.replace(val, "[password]", vmta.password);
            if (staticDomain != null && !"".equalsIgnoreCase(staticDomain)) {
                val = StringUtils.replace(val, "[rdns]", staticDomain);
                val = StringUtils.replace(val, "[domain]", staticDomain);
            } else {
                val = StringUtils.replace(val, "[rdns]", vmta.domain);
                val = StringUtils.replace(val, "[domain]", Domains.getDomainName(vmta.domain));
            }
            if (email != null) {
                val = StringUtils.replace(val, "[email_id]", String.valueOf(email.id));
                val = StringUtils.replace(val, "[list_id]", String.valueOf(email.listId));
                val = StringUtils.replace(val, "[email]", email.email);
                val = StringUtils.replace(val, "[fname]", email.fname);
                val = StringUtils.replace(val, "[lname]", email.lname);
                val = StringUtils.replace(val, "[email_name]", email.email.split("\\@")[0]);
            }
            if (mailDate != null && !"".equalsIgnoreCase(mailDate))
                val = StringUtils.replace(val, "[mail_date]", mailDate);
            if (messageId != null && !"".equalsIgnoreCase(messageId))
                val = StringUtils.replace(val, "[message_id]", messageId);
            val = StringUtils.replace(val, "[placeholder]", DropsSender.getCurrentPlaceHolder());
            val = DropsHelper.replaceRandomTags(val, this.drop.randomTags);
        }
        return val;
    }
}
