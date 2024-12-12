package org.jdkxx.cdc.common.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class InetAddressesUtils {
    public static String[] getInetAddresses() throws Exception {
        List<InetAddress> addresses = getInetAddressList();
        return addresses.stream().map(InetAddress::getHostAddress).toArray(String[]::new);
    }

    public static String[] getHostnames() throws Exception {
        List<InetAddress> addresses = getInetAddressList();
        return addresses.stream().map(InetAddress::getHostName).toArray(String[]::new);
    }

    public static String getInetAddress() throws Exception {
        String[] addresses = getInetAddresses();
        return addresses[addresses.length - 1];
    }

    private static List<InetAddress> getInetAddressList() throws Exception {
        List<InetAddress> addresses = new ArrayList<>(5);
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            Enumeration<InetAddress> inetAddresses = interfaces.nextElement().getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress inetAddress = inetAddresses.nextElement();
                if (!inetAddress.isLoopbackAddress()
                        && !inetAddress.isLinkLocalAddress()) {
                    addresses.add(inetAddress);
                }
            }
        }
        return addresses;
    }
}
