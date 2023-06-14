package org.apache.bookkeeper.bookie.util.testtypes;

import org.apache.bookkeeper.conf.ServerConfiguration;

public class InvalidServerConfig extends ServerConfiguration   {

    public InvalidServerConfig() {
        super();
    }

    @Override
    public int getBookiePort() {
        throw new RuntimeException("I am an invalid exception");
    }





}
