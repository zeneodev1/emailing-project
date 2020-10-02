package tech.bluemail.platform.remote;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import tech.bluemail.platform.logging.Logger;

public class SSH {
    private JSch jsch;

    private Session session;

    private String connectionType = "password";

    private String host;

    private String port = "22";

    private String username;

    private String password;

    private String rsaKey;

    public static SSH SSHPassword(String host, String port, String username, String password) {
        SSH ssh = new SSH();
        ssh.setHost(host);
        ssh.setPort(port);
        ssh.setUsername(username);
        ssh.setPassword(password);
        ssh.setConnectionType("password");
        return ssh;
    }

    public static SSH SSHKey(String host, String username, String port, String rsaKey) {
        SSH ssh = new SSH();
        ssh.setHost(host);
        ssh.setPort(port);
        ssh.setUsername(username);
        ssh.setRsaKey(rsaKey);
        ssh.setConnectionType("key");
        return ssh;
    }

    public void connect() {
        try {
            this.jsch = new JSch();
            if (this.connectionType.equalsIgnoreCase("password")) {
                this.session = this.jsch.getSession(this.username, this.host, Integer.parseInt(this.port));
                this.session.setPassword(this.password);
                this.session.setConfig("StrictHostKeyChecking", "no");
                this.session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            } else {
                this.jsch.addIdentity(this.rsaKey);
                this.session = this.jsch.getSession(this.username, this.host, Integer.parseInt(this.port));
                this.session.setConfig("StrictHostKeyChecking", "no");
                this.session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            }
            this.session.connect();
        } catch (Exception e) {
            Logger.error(e, SSH.class);
        }
    }

    public void disconnect() {
        if (isConnected()) {
            this.session.disconnect();
            this.jsch = null;
        }
    }

    public boolean isConnected() {
        return (this.session != null && this.session.isConnected());
    }

    public synchronized String cmd(String command) {
        String output = "";
        try {
            ChannelExec channelExec = (ChannelExec)this.session.openChannel("exec");
            InputStream in = channelExec.getInputStream();
            channelExec.setCommand(command);
            channelExec.connect();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null)
                output = output + line + "\n";
            channelExec.disconnect();
        } catch (Exception e) {
            Logger.error(e, SSH.class);
        }
        return output;
    }

    public synchronized List<String> cmdLines(String command) {
        List<String> lines = new ArrayList<>();
        try {
            ChannelExec channelExec = (ChannelExec)this.session.openChannel("exec");
            InputStream in = channelExec.getInputStream();
            channelExec.setCommand(command);
            channelExec.connect();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!"".equals(line.replaceAll("(?m)^[ \t]*\r?\n", "")))
                    lines.add(line);
            }
            channelExec.disconnect();
        } catch (Exception e) {
            Logger.error(e, SSH.class);
        }
        return lines;
    }

    public synchronized void downloadFile(String remotePath, String localPath) {
        if (isConnected())
            try {
                ChannelSftp sftpChannel = (ChannelSftp)this.session.openChannel("sftp");
                sftpChannel.connect();
                InputStream out = sftpChannel.get(remotePath);
                BufferedReader br = new BufferedReader(new InputStreamReader(out));
                try {
                    List<String> lines = new ArrayList<>();
                    String line;
                    while ((line = br.readLine()) != null)
                        lines.add(line);
                    FileUtils.writeLines(new File(localPath), lines);
                    br.close();
                } catch (Throwable throwable) {
                    try {
                        br.close();
                    } catch (Throwable throwable1) {
                        throwable.addSuppressed(throwable1);
                    }
                    throw throwable;
                }
                sftpChannel.disconnect();
            } catch (Exception e) {
                Logger.error(e, SSH.class);
            }
    }

    public synchronized void uploadFile(String localPath, String remotePath) {
        if (isConnected())
            try {
                File file = new File(localPath);
                if (!file.isDirectory() && file.exists()) {
                    InputStream is = new FileInputStream(file);
                    try {
                        ChannelSftp sftpChannel = (ChannelSftp)this.session.openChannel("sftp");
                        sftpChannel.connect();
                        sftpChannel.put(is, remotePath);
                        sftpChannel.disconnect();
                        is.close();
                    } catch (Throwable throwable) {
                        try {
                            is.close();
                        } catch (Throwable throwable1) {
                            throwable.addSuppressed(throwable1);
                        }
                        throw throwable;
                    }
                }
            } catch (Exception e) {
                Logger.error(e, SSH.class);
            }
    }

    public synchronized void uploadFile(String localPath, String remotePath, ChannelSftp sftpChannel) {
        if (isConnected())
            try {
                File file = new File(localPath);
                if (!file.isDirectory() && file.exists()) {
                    InputStream is = new FileInputStream(file);
                    try {
                        sftpChannel.put(is, remotePath);
                        is.close();
                    } catch (Throwable throwable) {
                        try {
                            is.close();
                        } catch (Throwable throwable1) {
                            throwable.addSuppressed(throwable1);
                        }
                        throw throwable;
                    }
                }
            } catch (Exception e) {
                Logger.error(e, SSH.class);
            }
    }

    public synchronized void uploadContent(String content, String remotePath) {
        if (isConnected())
            try {
                ChannelSftp sftpChannel = (ChannelSftp)this.session.openChannel("sftp");
                sftpChannel.connect();
                InputStream is = new ByteArrayInputStream(content.getBytes());
                try {
                    sftpChannel.put(is, remotePath);
                    is.close();
                } catch (Throwable throwable) {
                    try {
                        is.close();
                    } catch (Throwable throwable1) {
                        throwable.addSuppressed(throwable1);
                    }
                    throw throwable;
                }
                sftpChannel.disconnect();
            } catch (Exception e) {
                Logger.error(e, SSH.class);
            }
    }

    public synchronized String readContent(String remotePath) {
        String lines = "";
        if (isConnected())
            try {
                ChannelSftp sftpChannel = (ChannelSftp)this.session.openChannel("sftp");
                sftpChannel.connect();
                InputStream out = sftpChannel.get(remotePath);
                BufferedReader br = new BufferedReader(new InputStreamReader(out));
                try {
                    String line;
                    while ((line = br.readLine()) != null)
                        lines = lines + line;
                    br.close();
                } catch (Throwable throwable) {
                    try {
                        br.close();
                    } catch (Throwable throwable1) {
                        throwable.addSuppressed(throwable1);
                    }
                    throw throwable;
                }
                sftpChannel.disconnect();
            } catch (Exception e) {
                Logger.error(e, SSH.class);
            }
        return lines;
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return this.port;
    }

    public void setPort(String port) {
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

    public String getRsaKey() {
        return this.rsaKey;
    }

    public void setRsaKey(String rsaKey) {
        this.rsaKey = rsaKey;
    }

    public JSch getJsch() {
        return this.jsch;
    }

    public void setJsch(JSch jsch) {
        this.jsch = jsch;
    }

    public Session getSession() {
        return this.session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public String getConnectionType() {
        return this.connectionType;
    }

    public void setConnectionType(String connectionType) {
        this.connectionType = connectionType;
    }
}
