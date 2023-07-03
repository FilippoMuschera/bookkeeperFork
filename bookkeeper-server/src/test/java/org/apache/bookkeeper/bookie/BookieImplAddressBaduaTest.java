package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.util.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookieImplAddressBaduaTest {

    @Test
    public void unresolvedHostNameTest() {
        try (MockedStatic<DNS> mockedStatic = Mockito.mockStatic(DNS.class)) {
            mockedStatic.when(() -> DNS.getDefaultHost("unresolved")).thenReturn("unresolved.host.name");

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setAdvertisedAddress("");
            conf.setListeningInterface("unresolved");
            conf.setBookiePort(0);

            boolean exceptionThrown = false;
            BookieSocketAddress bookieSocketAddress = null;

            try {
                bookieSocketAddress = BookieImpl.getBookieAddress(conf);
            } catch (UnknownHostException e) {
                exceptionThrown = true;

                assertTrue(e.getMessage().contains("Unable to resolve default hostname"));
                assertTrue(e.getMessage().contains("unresolved.host.name"));
            }

            assertTrue(exceptionThrown);

        }
    }

    @Test
    public void failDefaultHostNameTest() {
        try (MockedStatic<DNS> mockedStatic = Mockito.mockStatic(DNS.class)) {
            mockedStatic.when(() -> DNS.getDefaultHost("default")).thenReturn("unresolved.host.name");

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setAdvertisedAddress("");
            conf.setListeningInterface(null);
            conf.setBookiePort(0);

            boolean exceptionThrown = false;
            BookieSocketAddress bookieSocketAddress = null;

            try {
                bookieSocketAddress = BookieImpl.getBookieAddress(conf);
            } catch (UnknownHostException e) {
                exceptionThrown = true;

                assertTrue(e.getMessage().contains("Unable to resolve default hostname"));
                assertTrue(e.getMessage().contains("default"));
            }

            assertTrue(exceptionThrown);

        }
    }


}
