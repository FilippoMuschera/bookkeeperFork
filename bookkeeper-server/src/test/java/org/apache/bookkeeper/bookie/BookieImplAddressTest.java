package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.assertFalse;

@RunWith(Parameterized.class)
public class BookieImplAddressTest {

    private static int testIndex = 0;

    private BookieSocketAddress bookieSocketAddress = null;
    private ServerConfiguration anotherConf;

    private ExpectedValue expectedValue;

    private final String HOST_NAME = InetAddress.getLocalHost().getCanonicalHostName();

    public BookieImplAddressTest(String address, int port, InterfaceType interfaceType, boolean hostAsName, boolean shortName, boolean loopback, ExpectedValue expectedValue) throws UnknownHostException, SocketException {
        anotherConf = new ServerConfiguration();
        anotherConf.setAdvertisedAddress(address);
        anotherConf.setBookiePort(port);
        anotherConf.setListeningInterface(getInterfaceName(interfaceType));
        anotherConf.setUseHostNameAsBookieID(hostAsName);
        anotherConf.setUseShortHostName(shortName);
        anotherConf.setAllowLoopback(loopback);
        this.expectedValue = expectedValue;
        testIndex += 1;

    }

    private String getInterfaceName(InterfaceType interfaceType) throws SocketException, UnknownHostException {

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

    @Parameterized.Parameters
    public static Collection<Object[]> getParams() {
        return Arrays.asList(new Object[][] {
                //address    //port    //interface              //hostNameAsBookie    //shortName    //allowLoopBack    //expectedValue
                {"",           1,     InterfaceType.NULL,           false,                false,         true,            ExpectedValue.PASSED},
                {"",           1,     InterfaceType.INVALID,        false,                false,         true,            ExpectedValue.UH_EXCEPTION},
                {"",           0,     InterfaceType.VALID,          true,                 true,          true,            ExpectedValue.PASSED},
                {"",           0,     InterfaceType.VALID,          true,                 false,         false,           ExpectedValue.UH_EXCEPTION},
                {"",           1,     InterfaceType.EMPTY,          false,                false,         true,            ExpectedValue.UH_EXCEPTION},
         {"0.0.0.0",           1,     InterfaceType.VALID,          false,                false,         true,            ExpectedValue.PASSED },
         {"0.0.0.0",           1,     InterfaceType.VALID,          false,                false,         false,           ExpectedValue.PASSED },
         {"192.168.56.102",    1,     InterfaceType.VALID,          false,                false,         true,            ExpectedValue.PASSED },
         {"192.168.56.102",    1,     InterfaceType.VALID,          true,                 true,          false,           ExpectedValue.PASSED},
                {null,         1,     InterfaceType.VALID,          false,                false,         true,            ExpectedValue.PASSED},
                {"",           -1,    InterfaceType.VALID,          true,                 false,         true,            ExpectedValue.IA_EXCEPTION},
                {"",         65536,   InterfaceType.VALID,          true,                 false,         true,            ExpectedValue.IA_EXCEPTION},
         {"300.598.1.2",      1,      InterfaceType.VALID,          false,                false,         true,            ExpectedValue.PASSED},


        });
    }

    @Test
    public void getAddressTest() {
        ExpectedValue actualValue = ExpectedValue.PASSED;

        try {
            bookieSocketAddress = BookieImpl.getBookieAddress(anotherConf);
            Assert.assertEquals("La porta passata come parametro è diversa da quella effettiva del bookieSocketAddress", anotherConf.getBookiePort(), bookieSocketAddress.getPort());
            if (anotherConf.getAdvertisedAddress() != null && anotherConf.getAdvertisedAddress().trim().length() > 0) {
                Assert.assertEquals(anotherConf.getAdvertisedAddress(), bookieSocketAddress.getHostName());
            }

            else if (anotherConf.getUseHostNameAsBookieID()) {
                if (anotherConf.getUseShortHostName()) {
                    Assert.assertEquals(InetAddress.getLocalHost().getCanonicalHostName().split("\\.", 2)[0], bookieSocketAddress.getHostName());
                }
                else {
                    Assert.assertEquals(HOST_NAME, bookieSocketAddress.getHostName());
                }
            }
            else {
                if (anotherConf.getAdvertisedAddress() == null || anotherConf.getAdvertisedAddress().equals("")) {
                    Assert.assertEquals(InetAddress.getLocalHost().getHostAddress(), bookieSocketAddress.getHostName());

                }
                else {
                    Assert.assertEquals(anotherConf.getAdvertisedAddress(), bookieSocketAddress.getHostName());

                }
            }

        } catch (UnknownHostException e) {
            actualValue = ExpectedValue.UH_EXCEPTION;
            System.out.println(e.toString());
        } catch (IllegalArgumentException e) {
            actualValue = ExpectedValue.IA_EXCEPTION;
            System.out.println(e.toString());

        }
        Assert.assertEquals("Test #" + testIndex + " failed (index starts from 1)" ,expectedValue, actualValue);
    }

    private enum ExpectedValue {
        PASSED, UH_EXCEPTION, IA_EXCEPTION
    }

    private enum InterfaceType {
        NULL, VALID, INVALID, EMPTY
    }

}