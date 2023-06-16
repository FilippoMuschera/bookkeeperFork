package org.apache.bookkeeper.bookie.util.testtypes;

import org.apache.bookkeeper.util.DiskChecker;

import java.io.File;
import java.util.List;

public class InvalidDiskChecker extends DiskChecker {
    public InvalidDiskChecker(float threshold, float warnThreshold) {
        super(threshold, warnThreshold);
    }

    @Override
    public long getTotalFreeSpace(List<File> dirs) {
        return -1L;
    }

    @Override
    public long getTotalDiskSpace(List<File> dirs) {
        return -1L;
    }

    @Override
    public float getTotalDiskUsage(List<File> dirs) {
        return -1.1f;
    }

    @Override
    public float checkDir(File dir) throws DiskErrorException {
        throw new DiskErrorException("Eccezione generata da InvalidRegistrationManager");
    }
}
