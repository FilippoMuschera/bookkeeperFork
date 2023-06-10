package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.util.TestBookieImplUtil;
import org.apache.bookkeeper.bookie.util.testtypes.InputBundle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.bookkeeper.bookie.BookieImplTest.generateIndexDirs;
import static org.apache.bookkeeper.bookie.BookieImplTest.getCorrespondingLedgerDirsManager;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class BookieImplFailureTest {

    private static final List<File> dirs = new ArrayList<>();
    private final String LEDGER_STRING = "ledger";
    private final int flushInterval = 1000;
    private final InputBundle bundle;
    ServerConfiguration conf = new ServerConfiguration();
    RegistrationManager reg;
    LedgerStorage ledgerStorage;
    DiskChecker diskChecker;
    File[] indexDirs;
    LedgerDirsManager ledgerDirsManager;
    LedgerDirsManager indexDirsManager;
    StatsLogger statsLogger;
    ByteBufAllocator byteBufAllocator;
    Supplier<BookieServiceInfo> bookieServiceInfo;
    private BookieImpl bookieImpl;
    private File[] ledgerFiles = new File[]{};


    public BookieImplFailureTest(InputBundle bundle) throws IOException {

        this.bundle = bundle;
        conf.setBookiePort(bundle.bookiePort);
        String[] jourDirs;
        if (bundle.journalDirs == TestBookieImplUtil.DataType.EMPTY)
            jourDirs = new String[]{};
        else if (bundle.journalDirs == TestBookieImplUtil.DataType.NULL)
            jourDirs = null;
        else {
            jourDirs = generateTempDirs(3, "journal");
            if (bundle.journalDirs == TestBookieImplUtil.DataType.INVALID)
                jourDirs[0] = "notAPath\0"; //Invalidiamo uno dei path

        }
        String[] ledgDirs;
        if (bundle.ledgDirs == TestBookieImplUtil.DataType.EMPTY)
            ledgDirs = new String[]{};
        else if (bundle.ledgDirs == TestBookieImplUtil.DataType.NULL)
            ledgDirs = null;
        else {
            ledgDirs = generateTempDirs(3, LEDGER_STRING);
            if (bundle.ledgDirs == TestBookieImplUtil.DataType.INVALID)
                ledgDirs[0] = "notAPath\0"; //Invalidiamo uno dei path

        }
        conf.setJournalDirsName(jourDirs);
        conf.setLedgerDirNames(ledgDirs);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setDiskUsageThreshold(0.99f);
        conf.setFlushInterval(bundle.flushInt);
        conf.setEntryLogPerLedgerEnabled(bundle.logPerLedger);
        conf.getLedgerDirNames();

        reg = bundle.registrationManager;
        ledgerStorage = bundle.ledgerStorage;
        diskChecker = bundle.diskChecker;
        indexDirs = generateIndexDirs(3);
        ledgerDirsManager = getCorrespondingLedgerDirsManager(bundle.ledgerDirsManagerType, conf, indexDirs, diskChecker);
        indexDirsManager = getCorrespondingLedgerDirsManager(bundle.indexDirsManager, conf, indexDirs, diskChecker);
        statsLogger = bundle.statsLogger;
        byteBufAllocator = bundle.byteBufAllocator;
        bookieServiceInfo = bundle.bookieServiceInfoSupplier;


    }


    @Parameterized.Parameters
    public static Collection<InputBundle> getParams() throws IOException {
        List<InputBundle> bundles = new ArrayList<>();
        bundles.add(InputBundle.getDefault().setBookiePort(-1).setExpectedValue(TestBookieImplUtil.ExpectedValue.IA_EXCEPTION));
        bundles.add(InputBundle.getDefault().setJournalDirs(TestBookieImplUtil.DataType.INVALID).setExpectedValue(TestBookieImplUtil.ExpectedValue.IO_EXCEPTION));
        //bundles.add(InputBundle.getDefault().setLedgDirs(TestBookieImplUtil.DataType.INVALID).setExpectedValue(TestBookieImplUtil.ExpectedValue.IO_EXCEPTION));
        bundles.add(InputBundle.getDefault().setFlushInt(0).setExpectedValue(TestBookieImplUtil.ExpectedValue.IA_EXCEPTION));
        bundles.add(InputBundle.getDefault().setFlushInt(-1).setExpectedValue(TestBookieImplUtil.ExpectedValue.IA_EXCEPTION));






        return bundles;
    }


    @Test
    public void setUpBookieTest() throws InterruptedException, BookieException {
        TestBookieImplUtil.ExpectedValue expectedValue;

       try {
            ledgerStorage.initialize(conf, new NullMetadataBookieDriver.NullLedgerManager(), ledgerDirsManager, indexDirsManager,
                    statsLogger, byteBufAllocator);


            this.bookieImpl = new BookieImpl(
                    conf,
                    reg,
                    ledgerStorage,
                    diskChecker,
                    ledgerDirsManager,
                    indexDirsManager,
                    statsLogger,
                    byteBufAllocator,
                    bookieServiceInfo
            );

            bookieImpl.start();

            assertTrue(bookieImpl.isRunning());
            assertFalse(bookieImpl.isReadOnly());
            assertTrue(bookieImpl.getTotalDiskSpace() >= bookieImpl.getTotalFreeSpace());
            assertTrue(bookieImpl.isAvailableForHighPriorityWrites());
            List<File> currDirs = new ArrayList<>(Arrays.asList(BookieImpl.getCurrentDirectories(ledgerFiles)));
            for (File ledgerFile : ledgerFiles)
                assertTrue(currDirs.contains(BookieImpl.getCurrentDirectory(ledgerFile)));
            assertTrue(BookieImpl.getBookieId(conf).toString().contains(Integer.toString(conf.getBookiePort())));
            addEntry();
        } catch (IllegalArgumentException iae) {
           expectedValue = TestBookieImplUtil.ExpectedValue.IA_EXCEPTION;
           assertEquals(expectedValue, bundle.expectedValue);
           return;
       } catch (IOException ioe) {
           expectedValue = TestBookieImplUtil.ExpectedValue.IO_EXCEPTION;
           assertEquals(expectedValue, bundle.expectedValue);
           return;
       }

       fail("temporary fail");

    }


    private void addEntry() throws IOException, InterruptedException, BookieException {
        ByteBuf buffer = Unpooled.buffer();
        long ledgerId = 1;
        long entry = 0;
        long lastConf = -1;
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        String expectedCtx = "foo";


        bookieImpl.addEntry(buffer, true, (int rc, long ledgerId1, long entryId1,
                                           BookieId addr, Object ctx) -> {
            assertSame(expectedCtx, ctx);
            assertEquals(ledgerId, ledgerId1);
            assertEquals(entry, entryId1);
        }, "foo", "key".getBytes());

    }

    private String[] generateTempDirs(int n, String suffix) throws IOException {
        if (suffix.equals(LEDGER_STRING)) {
            ledgerFiles = new File[n];
        }
        String[] ret = new String[n];
        for (int i = 0; i < n; i++) {
            Path path = Files.createTempDirectory(suffix + i);
            dirs.add(path.toFile());
            ret[i] = path.toFile().getPath();
            if (suffix.equals(LEDGER_STRING)) {
                ledgerFiles[i] = path.toFile();
            }
        }

        return ret;
    }

}
