package org.apache.bookkeeper.bookie.util.testtypes;

import io.netty.buffer.ByteBufAllocator;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;

import java.util.function.Supplier;

import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.DataType;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.ExpectedValue;

public class InputBundle {

    public int bookiePort;
    public DataType journalDirs;
    public DataType ledgDirs;
    public int flushInt;
    public boolean logPerLedger;
    public RegistrationManager registrationManager;
    public LedgerStorage ledgerStorage;
    public DiskChecker diskChecker;
    public DataType ledgerDirsManagerType;
    public DataType indexDirs;
    public DataType indexDirsManager;
    public StatsLogger statsLogger;
    public ByteBufAllocator byteBufAllocator;
    public Supplier<BookieServiceInfo> bookieServiceInfoSupplier;
    public ExpectedValue expectedValue;

    public InputBundle(int bookiePort, DataType journalDirs, DataType ledgDirs, int flushInt, boolean logPerLedger,
                       RegistrationManager registrationManager, LedgerStorage ledgerStorage,
                       DiskChecker diskChecker, DataType ledgerDirsManager, DataType indexDirs,
                       DataType indexDirsManager, StatsLogger statsLogger, ByteBufAllocator byteBufAllocator,
                       Supplier<BookieServiceInfo> bookieServiceInfoSupplier, ExpectedValue expectedValue) {
        this.bookiePort = bookiePort;
        this.journalDirs = journalDirs;
        this.ledgDirs = ledgDirs;
        this.flushInt = flushInt;
        this.logPerLedger = logPerLedger;
        this.registrationManager = registrationManager;
        this.ledgerStorage = ledgerStorage;
        this.diskChecker = diskChecker;
        this.ledgerDirsManagerType = ledgerDirsManager;
        this.indexDirs = indexDirs;
        this.indexDirsManager = indexDirsManager;
        this.statsLogger = statsLogger;
        this.byteBufAllocator = byteBufAllocator;
        this.bookieServiceInfoSupplier = bookieServiceInfoSupplier;
        this.expectedValue = expectedValue;
    }
}
