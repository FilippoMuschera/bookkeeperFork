package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.util.TestAddressUtil;
import org.apache.bookkeeper.bookie.util.TestBKConfiguration;
import org.apache.bookkeeper.bookie.util.testtypes.CustomBookieSocketAddress;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;

import static org.apache.bookkeeper.bookie.util.TestAddressUtil.getInterfaceName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.powermock.api.mockito.PowerMockito.spy;

@PrepareForTest({DNS.class, BookieImpl.class})
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class BookieImplAddressMockTest {

    private final String addr = InetAddress.getLocalHost().getCanonicalHostName() + ":0";
    private final String LB_MSG =  "Trying to listen on loopback address, "
            + addr + " but this is forbidden by default "
            + "(see ServerConfiguration#getAllowLoopback()).\n"
            + "If this happen, you can consider specifying the network interface"
            + " to listen on (e.g. listeningInterface=eth0) or specifying the"
            + " advertised address (e.g. advertisedAddress=172.x.y.z)";

    public BookieImplAddressMockTest() throws UnknownHostException {
    }

    @Test
    public void testUnknownInterface() throws Exception {

        PowerMockito.spy(DNS.class);
        PowerMockito.when(DNS.class, "getSubinterface", "notAnInterface").thenAnswer((Answer<NetworkInterface>) invocation -> null);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setAdvertisedAddress("");
        conf.setListeningInterface("notAnInterface");
        conf.setBookiePort(1);
        conf.setUseHostNameAsBookieID(false);
        conf.setUseShortHostName(false);
        conf.setAllowLoopback(true);
        boolean exceptionThrown = false;
        BookieSocketAddress bookieSocketAddress = null;

        try {
            bookieSocketAddress = BookieImpl.getBookieAddress(conf);
        } catch (UnknownHostException e) {
            exceptionThrown = true;
            assertEquals("No such interface notAnInterface", e.getMessage());
        }

        assertTrue(exceptionThrown);

    }

    @Test
    public void testLoopBackException() throws Exception {


        BookieSocketAddress myBookie = new CustomBookieSocketAddress(InetAddress.getLocalHost().getCanonicalHostName(), 0);
        PowerMockito.whenNew(BookieSocketAddress.class).withAnyArguments().thenReturn(myBookie);
        BookieSocketAddress anotherOne = new BookieSocketAddress("filippoVirtualbox", 2020);
        System.out.println(anotherOne.getSocketAddress().getAddress().getHostAddress());

        //controlla che sto coso fa quello che deve

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setAdvertisedAddress("");
        conf.setListeningInterface(getInterfaceName(TestAddressUtil.InterfaceType.VALID));
        conf.setBookiePort(0);
        conf.setUseHostNameAsBookieID(true);
        conf.setUseShortHostName(false);
        conf.setAllowLoopback(false);
        boolean exceptionThrown = false;
        BookieSocketAddress bookieSocketAddress = null;

        try {
            bookieSocketAddress = BookieImpl.getBookieAddress(conf);
        } catch (UnknownHostException e) {
            exceptionThrown = true;

            assertEquals(LB_MSG, e.getMessage());
        }

        assertTrue(exceptionThrown);

    }









}