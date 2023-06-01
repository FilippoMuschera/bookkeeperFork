package org.apache.bookkeeper.bookie.util;

import org.apache.bookkeeper.bookie.BookieImplAddressTest;
import org.apache.bookkeeper.net.DNS;

import java.net.*;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestAddressUtil {

    public static String getInterfaceName(InterfaceType interfaceType) throws SocketException, UnknownHostException {

        boolean exceptionThrown = false;
        try {
            switch (interfaceType) {
                case NULL:
                    return null;
                case EMPTY:
                    return "";
                case INVALID:
                    return "notAnInterface";
                case VALID:
                    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                    String address = InetAddress.getLocalHost().getHostAddress();
                    while (interfaces.hasMoreElements()) {
                        NetworkInterface iface = interfaces.nextElement();
                        String host = DNS.getDefaultHost(iface.getName());
                        String ip = new InetSocketAddress(host, 0).getAddress().getHostAddress();

                        if (address.equals(ip))
                            return iface.getName();
                    }
                    throw new RuntimeException("errore nel reperire la corretta interfaccia di rete");

            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);
        return null;
    }


    public enum ExpectedValue {
        PASSED, UH_EXCEPTION, IA_EXCEPTION
    }

    public enum InterfaceType {
        NULL, VALID, INVALID, EMPTY
    }




}
