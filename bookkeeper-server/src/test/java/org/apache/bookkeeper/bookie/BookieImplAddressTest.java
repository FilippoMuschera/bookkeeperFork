package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.util.TestBookieImplUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.*;

@RunWith(Parameterized.class)
public class BookieImplAddressTest {

    private static int testIndex = 0;

    private BookieSocketAddress bookieSocketAddress = null;
    private final ServerConfiguration config;

    private final TestBookieImplUtil.ExpectedValue expectedValue;

    private final String HOST_NAME = InetAddress.getLocalHost().getCanonicalHostName();

    public BookieImplAddressTest(String address, int port, DataType dataType, boolean hostAsName, boolean shortName, boolean loopback, TestBookieImplUtil.ExpectedValue expectedValue) throws UnknownHostException, SocketException {
        config = new ServerConfiguration();
        config.setAdvertisedAddress(address);
        config.setBookiePort(port);
        config.setListeningInterface(getInterfaceName(dataType));
        config.setUseHostNameAsBookieID(hostAsName);
        config.setUseShortHostName(shortName);
        config.setAllowLoopback(loopback);
        this.expectedValue = expectedValue;
        testIndex += 1;

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParams() {
        return Arrays.asList(new Object[][] {
                //address    //port    //interface              //hostNameAsBookie    //shortName    //allowLoopBack    //expectedValue
                {"",           1,     DataType.NULL,           true,                 false,         true,            ExpectedValue.PASSED},
                {"",           0,     DataType.VALID,          true,                 true,          true,            ExpectedValue.PASSED},
                {"",           1,     DataType.EMPTY,          false,                false,         true,            ExpectedValue.UH_EXCEPTION},
         {"0.0.0.0",           1,     DataType.VALID,          false,                false,         true,            ExpectedValue.PASSED },
         {"0.0.0.0",           1,     DataType.VALID,          false,                false,         false,           ExpectedValue.PASSED },
         {"192.168.56.102",    1,     DataType.VALID,          false,                false,         true,            ExpectedValue.PASSED },
         {"192.168.56.102",    1,     DataType.VALID,          true,                 true,          false,           ExpectedValue.PASSED},
                {null,         1,     DataType.VALID,          false,                false,         true,            ExpectedValue.PASSED},
                {"",           -1,    DataType.VALID,          true,                 false,         true,            ExpectedValue.IA_EXCEPTION},
                {"",         65536,   DataType.VALID,          true,                 false,         true,            ExpectedValue.IA_EXCEPTION},
         {"30000.598.1.2",      1,    DataType.VALID,          false,                false,         true,            ExpectedValue.PASSED},


        });
    }

    @Test
    public void getAddressTest() {
        ExpectedValue actualValue = ExpectedValue.PASSED;

        try {
            bookieSocketAddress = BookieImpl.getBookieAddress(config);
            Assert.assertEquals("La porta passata come parametro è diversa da quella effettiva del bookieSocketAddress", config.getBookiePort(), bookieSocketAddress.getPort());
            if (config.getAdvertisedAddress() != null && config.getAdvertisedAddress().trim().length() > 0) {
                Assert.assertEquals(config.getAdvertisedAddress(), bookieSocketAddress.getHostName());
            }

            else if (config.getUseHostNameAsBookieID()) {
                if (config.getUseShortHostName()) {
                    Assert.assertEquals(InetAddress.getLocalHost().getCanonicalHostName().split("\\.", 2)[0], bookieSocketAddress.getHostName());
                }
                else {
                    Assert.assertEquals(HOST_NAME, bookieSocketAddress.getHostName());
                }
            }
            else {
                if (config.getAdvertisedAddress() == null || config.getAdvertisedAddress().equals("")) {
                    Assert.assertEquals(InetAddress.getLocalHost().getHostAddress(), bookieSocketAddress.getHostName());

                }
                else {
                    Assert.assertEquals(config.getAdvertisedAddress(), bookieSocketAddress.getHostName());

                }
            }

        } catch (UnknownHostException e) {
            actualValue = ExpectedValue.UH_EXCEPTION;
            System.out.println(e);
        } catch (IllegalArgumentException e) {
            actualValue = ExpectedValue.IA_EXCEPTION;
            System.out.println(e);

        }
        Assert.assertEquals("Test #" + testIndex + " failed (index starts from 1)" ,expectedValue, actualValue);
    }



}