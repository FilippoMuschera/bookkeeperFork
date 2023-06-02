package org.apache.bookkeeper.bookie.util.testtypes;

import org.apache.bookkeeper.net.BookieSocketAddress;

import java.net.InetSocketAddress;

public class CustomBookieSocketAddress extends BookieSocketAddress {


    private InetSocketAddress loopbackSocketAddress;
    public CustomBookieSocketAddress(String hostname, int port) {
        super(hostname, port);
        this.setUpMockedAddress();

    }

    private void setUpMockedAddress() {
        loopbackSocketAddress = new InetSocketAddress("localhost", 8080);
        if (!loopbackSocketAddress.getAddress().isLoopbackAddress()) {
            System.out.println("****ADDRESS NON LOOPBACK****");
            throw new RuntimeException();
        }
    }

    @Override
    public InetSocketAddress getSocketAddress() {

        return this.loopbackSocketAddress;

    }
}

