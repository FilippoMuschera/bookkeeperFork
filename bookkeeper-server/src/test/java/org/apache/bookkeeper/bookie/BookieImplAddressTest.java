package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BookieImplAddressTest {


    private BookieSocketAddress bookieSocketAddress = null;
    private ServerConfiguration anotherConf;

    private ExpectedValue expectedValue;

    private final String HOST_NAME = InetAddress.getLocalHost().getHostName();

    public BookieImplAddressTest(String address, int port, String interfaceName, boolean hostAsName, boolean shortName, boolean loopback, ExpectedValue expectedValue) throws UnknownHostException {
        anotherConf = new ServerConfiguration();
        anotherConf.setAdvertisedAddress(address);
        anotherConf.setBookiePort(port);
        anotherConf.setListeningInterface(interfaceName); //enp0s3 enp0s8 enp0s9 enp0s810
        anotherConf.setUseHostNameAsBookieID(hostAsName);
        anotherConf.setUseShortHostName(shortName);
        anotherConf.setAllowLoopback(loopback);
        this.expectedValue = expectedValue;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParams() {
        return Arrays.asList(new Object[][] {
                //address    //port    //interface      //hostNameAsBookie    //shortName    //allowLoopBack    //expectedValue
                {"",           1,       null,            false,                false,         true,            ExpectedValue.PASSED},
                {"",           1,     "notAnInterface",  false,                false,         true,            ExpectedValue.UH_EXCEPTION},
                {"",           0,     "enp0s9",          true,                true,         true,            ExpectedValue.PASSED},
                {"",           0,     "enp0s9",          true,                false,         false,           ExpectedValue.UH_EXCEPTION},
                {"",           1,     "",                false,                false,         true,            ExpectedValue.UH_EXCEPTION},
         {"0.0.0.0",           1,     "enp0s9",          false,                 false,          true,            ExpectedValue.PASSED },
         {"0.0.0.0",           1,     "enp0s9",          false,                 false,          false,            ExpectedValue.PASSED },
         {"192.168.56.102",    1,     "enp0s9",          false,                 false,          true,            ExpectedValue.PASSED },
         {"192.168.56.102",    1,     "enp0s9",          true,                 true,          false,           ExpectedValue.PASSED},
                {null,         1,     "enp0s9",          false,                false,         true,            ExpectedValue.PASSED},
                {"",           -1,    "enp0s9",          true,                 false,         true,            ExpectedValue.IA_EXCEPTION},
                {"",         65536,   "enp0s9",          true,                 false,         true,            ExpectedValue.IA_EXCEPTION},
         {"300.598.1.2",      1,      "enp0s9",          false,                false,         true,            ExpectedValue.PASSED},


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
        Assert.assertEquals(expectedValue, actualValue);
    }

    private enum ExpectedValue {
        PASSED, UH_EXCEPTION, IA_EXCEPTION
    }

}