package org.apache.bookkeeper.bookie.util.testtypes;

import org.apache.bookkeeper.util.DiskChecker;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class InvalidDiskChecker extends DiskChecker {
    public InvalidDiskChecker(float threshold, float warnThreshold) {
        super(threshold, warnThreshold);
    }

    @Override
    public long getTotalFreeSpace(List<File> dirs) throws IOException {
        return -1L;
    }

    @Override
    public long getTotalDiskSpace(List<File> dirs) throws IOException {
        return -1L;
    }

    @Override
    public float getTotalDiskUsage(List<File> dirs) throws IOException {
        return -1.1f;
    }

    @Override
    public float checkDir(File dir) throws DiskErrorException, DiskOutOfSpaceException, DiskWarnThresholdException {
        throw new DiskErrorException("Eccezione generata da InvalidRegistrationManager");
    }
}
