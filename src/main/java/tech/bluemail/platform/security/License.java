package tech.bluemail.platform.security;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import tech.bluemail.platform.logging.Logger;

public class License {
    public static void check() throws Exception {}

    public static InetAddress getCurrentIp() {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = networkInterfaces.nextElement();
                Enumeration<InetAddress> nias = ni.getInetAddresses();
                while (nias.hasMoreElements()) {
                    InetAddress ia = nias.nextElement();
                    if (!ia.isLinkLocalAddress() &&
                            !ia.isLoopbackAddress() && ia instanceof java.net.Inet4Address)
                        return ia;
                }
            }
        } catch (SocketException e) {
            Logger.error(e, License.class);
        }
        return null;
    }
}
