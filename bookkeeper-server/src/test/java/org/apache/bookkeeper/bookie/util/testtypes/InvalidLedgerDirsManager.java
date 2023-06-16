package org.apache.bookkeeper.bookie.util.testtypes;

import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.util.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InvalidLedgerDirsManager extends LedgerDirsManager {

    List<File> illegalList = new ArrayList<>();
    private boolean shouldIThrow = false;
    public InvalidLedgerDirsManager() throws IOException {
        this(TestBKConfiguration.newServerConfiguration(), new File[] {}, new InvalidDiskChecker(0.5f, 0.5f));
        illegalList.add(new File("notAPath\0"));
    }
    public InvalidLedgerDirsManager(ServerConfiguration conf, File[] dirs, DiskChecker diskChecker) throws IOException {
        super(conf, dirs, diskChecker);
    }

    public InvalidLedgerDirsManager(boolean b) throws IOException { //Costruttore per fare in modo che getAllLedgerDirs() faccia il throw di un'eccezione
        //invece che il ritorno di un dato scorretto
        this();
        this.shouldIThrow = b;
    }

    @Override
    public List<File> getAllLedgerDirs() {
        if (shouldIThrow)
                throw new ArrayIndexOutOfBoundsException("Invalid Ledger Dirs Manager Instance!");
        else
                return illegalList;
    }

    @Override
    public List<LedgerDirsListener> getListeners() {
        return Collections.emptyList();
    }

    @Override
    public long getTotalFreeSpace(List<File> dirs) {
        return -1;
    }

    @Override
    public long getTotalDiskSpace(List<File> dirs) {
        return -1;
    }

    @Override
    public ConcurrentMap<File, Float> getDiskUsages() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public List<File> getWritableLedgerDirs() {
        return illegalList;

    }

    @Override
    public boolean hasWritableLedgerDirs() {
        return false;
    }

    @Override
    public List<File> getWritableLedgerDirsForNewLog() {
        return illegalList;

    }

    @Override
    public List<File> getFullFilledLedgerDirs() {
        return illegalList;

    }

    @Override
    public boolean isDirFull(File dir) {
        return true;
    }

    @Override
    public void addToFilledDirs(File dir) {
    }

    @Override
    public void addToWritableDirs(File dir, boolean underWarnThreshold) {
    }

    @Override
    public void addLedgerDirsListener(LedgerDirsListener listener) {
    }

    @Override
    public DiskChecker getDiskChecker() {
        return new InvalidDiskChecker(0.6f, 0.5f);
    }
}
