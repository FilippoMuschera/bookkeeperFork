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

import static java.util.Collections.emptyMap;

public class InvalidLedgerDirsManager extends LedgerDirsManager {

    List<File> illegalList = new ArrayList<>();
    public InvalidLedgerDirsManager() throws IOException {
        this(TestBKConfiguration.newServerConfiguration(), new File[] {}, new InvalidDiskChecker(0.5f, 0.5f));
        illegalList.add(new File("notAPath\0"));
    }
    public InvalidLedgerDirsManager(ServerConfiguration conf, File[] dirs, DiskChecker diskChecker) throws IOException {
        super(conf, dirs, diskChecker);
    }

    @Override
    public List<File> getAllLedgerDirs() {
        return illegalList;
    }

    @Override
    public List<LedgerDirsListener> getListeners() {
        return Collections.emptyList();
    }

    @Override
    public long getTotalFreeSpace(List<File> dirs) throws IOException {
        return -1;
    }

    @Override
    public long getTotalDiskSpace(List<File> dirs) throws IOException {
        return -1;
    }

    @Override
    public ConcurrentMap<File, Float> getDiskUsages() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public List<File> getWritableLedgerDirs() throws NoWritableLedgerDirException {
        return illegalList;

    }

    @Override
    public boolean hasWritableLedgerDirs() {
        return false;
    }

    @Override
    public List<File> getWritableLedgerDirsForNewLog() throws NoWritableLedgerDirException {
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
        return;
    }

    @Override
    public void addToWritableDirs(File dir, boolean underWarnThreshold) {
        return;
    }

    @Override
    public void addLedgerDirsListener(LedgerDirsListener listener) {
        return;
    }

    @Override
    public DiskChecker getDiskChecker() {
        return new InvalidDiskChecker(0.6f, 0.5f);
    }
}
