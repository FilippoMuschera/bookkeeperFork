package org.apache.bookkeeper.bookie.util.testtypes;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

public class InvalidRegistrationManager implements RegistrationManager {
    @Override
    public void close() {

    }

    @Override
    public String getClusterInstanceId() throws BookieException {
        throw new BookieException.BookieIllegalOpException();

    }

    @Override
    public void registerBookie(BookieId bookieId, boolean readOnly, BookieServiceInfo serviceInfo) throws BookieException {
        throw new BookieException.BookieIllegalOpException();

    }

    @Override
    public void unregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
        throw new BookieException.BookieIllegalOpException();

    }

    @Override
    public boolean isBookieRegistered(BookieId bookieId) throws BookieException {
        throw new BookieException.BookieIllegalOpException();

    }

    @Override
    public void writeCookie(BookieId bookieId, Versioned<byte[]> cookieData) throws BookieException {
        throw new BookieException.CookieExistException();

    }

    @Override
    public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
        throw new BookieException.InvalidCookieException();

    }

    @Override
    public void removeCookie(BookieId bookieId, Version version) throws BookieException {
        throw new BookieException.CookieNotFoundException();

    }

    @Override
    public boolean prepareFormat() throws Exception {
        throw new Exception();
    }

    @Override
    public boolean initNewCluster() throws Exception {
        throw new Exception();

    }

    @Override
    public boolean format() throws Exception {
        throw new Exception();

    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        throw new Exception();

    }

    @Override
    public void addRegistrationListener(RegistrationListener listener) {

        // do nothing
    }
}
