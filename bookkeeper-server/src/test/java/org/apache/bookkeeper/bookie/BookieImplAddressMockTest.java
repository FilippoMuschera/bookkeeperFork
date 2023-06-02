package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.util.TestBookieImplUtil;
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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;

import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.getInterfaceName;
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

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setAdvertisedAddress("");
        conf.setListeningInterface(getInterfaceName(TestBookieImplUtil.DataType.VALID));
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


 /*
 MOTIVO PER CUI IL BRANCH DEL LOOBACK RISULTERÀ COMUNQUE GIALLO IN JACOCO (Dalle FAQ di Jacoco https://www.jacoco.org/jacoco/trunk/doc/faq.html)

 Source code lines with exceptions show no coverage. Why?
JaCoCo determines code execution with so called probes.
Probes are inserted into the control flow at certain positions.
Code is considered as executed when a subsequent probe has been executed. In case of exceptions such a sequence of i
nstructions is aborted somewhere in the middle and the corresponding lines of source code are not marked as covered.
  */






}