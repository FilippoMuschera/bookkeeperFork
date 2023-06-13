package org.apache.bookkeeper.bookie.util.testtypes;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorageFactory;
import org.apache.bookkeeper.bookie.util.TestBKConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.proto.SimpleBookieServiceInfoProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;

import java.io.IOException;
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

    public static InputBundle getDefault() throws IOException   {

        return new InputBundle(
                1,
                DataType.VALID,
                DataType.VALID,
                1000,
                true,
                new NullMetadataBookieDriver.NullRegistrationManager(),
                LedgerStorageFactory.createLedgerStorage("org.apache.bookkeeper.bookie.SortedLedgerStorage"),
                new DiskChecker(0.99f, 0.98f),
                DataType.VALID,
                DataType.VALID,
                DataType.VALID,
                new NullStatsLogger(),
                UnpooledByteBufAllocator.DEFAULT,
                new SimpleBookieServiceInfoProvider(TestBKConfiguration.newServerConfiguration()),
                ExpectedValue.PASSED

        );
    }

    public InputBundle setBookiePort(int bookiePort) {
        this.bookiePort = bookiePort;
        return this;
    }

    public InputBundle setJournalDirs(DataType journalDirs) {
        this.journalDirs = journalDirs;
        return this;
    }

    public InputBundle setLedgDirs(DataType ledgDirs) {
        this.ledgDirs = ledgDirs;
        return this;
    }

    public InputBundle setFlushInt(int flushInt) {
        this.flushInt = flushInt;
        return this;
    }

    public InputBundle setLogPerLedger(boolean logPerLedger) {
        this.logPerLedger = logPerLedger;
        return this;
    }

    public InputBundle setRegistrationManager(RegistrationManager registrationManager) {
        this.registrationManager = registrationManager;
        return this;
    }

    public InputBundle setLedgerStorage(LedgerStorage ledgerStorage) {
        this.ledgerStorage = ledgerStorage;
        return this;
    }

    public InputBundle setDiskChecker(DiskChecker diskChecker) {
        this.diskChecker = diskChecker;
        return this;
    }

    public InputBundle setLedgerDirsManagerType(DataType ledgerDirsManagerType) {
        this.ledgerDirsManagerType = ledgerDirsManagerType;
        return this;
    }

    public InputBundle setIndexDirs(DataType indexDirs) {
        this.indexDirs = indexDirs;
        return this;
    }

    public InputBundle setIndexDirsManager(DataType indexDirsManager) {
        this.indexDirsManager = indexDirsManager;
        return this;
    }

    public InputBundle setStatsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        return this;
    }

    public InputBundle setByteBufAllocator(ByteBufAllocator byteBufAllocator) {
        this.byteBufAllocator = byteBufAllocator;
        return this;
    }

    public InputBundle setBookieServiceInfoSupplier(Supplier<BookieServiceInfo> bookieServiceInfoSupplier) {
        this.bookieServiceInfoSupplier = bookieServiceInfoSupplier;
        return this;
    }

    public InputBundle setExpectedValue(ExpectedValue expectedValue) {
        this.expectedValue = expectedValue;
        return this;
    }
}
