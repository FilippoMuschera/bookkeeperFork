package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.util.testtypes.InputBundle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.AfterClass;
import org.junit.Test;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class BookieImplIT {

    private static BookieImpl bookieImpl;
    private static final List<File> dirs = new ArrayList<>();

    private InputBundle bundle = InputBundle.getDefault();
    ServerConfiguration conf = new ServerConfiguration();
    RegistrationManager reg;
    LedgerStorage ledgerStorage;
    DiskChecker diskChecker;
    File[] indexDirs;
    LedgerDirsManager ledgerDirsManager;
    LedgerDirsManager indexDirsManager;
    StatsLogger statsLogger;
    private final String LEDGER_STRING = "ledger";

    ByteBufAllocator byteBufAllocator;
    Supplier<BookieServiceInfo> bookieServiceInfo;
    private File[] ledgerFiles = new File[]{};

    public BookieImplIT() throws IOException, InterruptedException, BookieException {
        conf.setBookiePort(bundle.bookiePort);
        String[] jourDirs = generateTempDirs(3, "journal");

        String[] ledgDirs;
        File[] ledgerDirsFiles = new File[0];

        ledgDirs = generateTempDirs(3, LEDGER_STRING);
        ledgerDirsFiles = new File[3];
        for (int i = 0; i < ledgDirs.length; i++) {
            ledgerDirsFiles[i] = new File(ledgDirs[i]);

        }


        conf.setJournalDirsName(jourDirs);
        conf.setLedgerDirNames(ledgDirs);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setDiskUsageThreshold(0.99f);
        conf.setFlushInterval(bundle.flushInt);
        conf.setEntryLogPerLedgerEnabled(bundle.logPerLedger);
        reg = bundle.registrationManager;
        ledgerStorage = bundle.ledgerStorage;
        diskChecker = bundle.diskChecker;
        indexDirs = generateIndexDirs(3);

        ledgerDirsManager = new LedgerDirsManager(conf, ledgerDirsFiles, diskChecker);
        indexDirsManager = new LedgerDirsManager(conf, indexDirs, diskChecker);
        statsLogger = bundle.statsLogger;
        byteBufAllocator = bundle.byteBufAllocator;
        bookieServiceInfo = bundle.bookieServiceInfoSupplier;

        ledgerStorage.initialize(conf, new NullMetadataBookieDriver.NullLedgerManager(), ledgerDirsManager, indexDirsManager,
                statsLogger, byteBufAllocator);
        bookieImpl = new BookieImpl(
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
    }

    @Test
    public void readWriteLedgerTest() throws IOException, InterruptedException, BookieException {
        bookieImpl.ledgerStorage.flush(); //Siccome non abbiamo garanzie sull'ordine di esecuzione dei test, per prima cosa
        //eseguiamo il flush dei ledger, per "ripulire" e preparare l'ambiente al test successivo.
        ByteBuf buffer = Unpooled.buffer(Long.BYTES * 3);
        long ledgerId = 1;
        long entry = 0;
        long lastConf = (long) (Math.random() * 10);
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        Object expectedCtx = "foo";

        bookieImpl.addEntry(buffer, true, (int rc, long ledgerId1, long entryId1,
                                           BookieId addr, Object ctx) -> {
            assertSame(expectedCtx, ctx);
            assertEquals(ledgerId, ledgerId1);
            assertEquals(entry, entryId1);
        }, "foo", "key".getBytes());
        //forziamo la scrittura della entry
        bookieImpl.forceLedger(ledgerId, new BookieImpl.NopWriteCallback(), expectedCtx);
        long actualLastAdd = bookieImpl.readLastAddConfirmed(ledgerId);
        assertEquals(lastConf, actualLastAdd);
        ByteBuf readBuf = bookieImpl.readEntry(ledgerId, entry);
        assertEquals(ledgerId, readBuf.getLong(0));
        assertEquals(entry, readBuf.getLong(Long.BYTES));
        assertEquals(lastConf, readBuf.getLong(2*Long.BYTES));

        System.out.println(bookieImpl.getListOfEntriesOfLedger(ledgerId).next());



    }

    @Test
    public void testLAC() throws IOException, InterruptedException, BookieException {
        bookieImpl.ledgerStorage.flush(); //Siccome non abbiamo garanzie sull'ordine di esecuzione dei test, per prima cosa
        //eseguiamo il flush dei ledger, per "ripulire" e preparare l'ambiente al test successivo.
        ByteBuf buffer = Unpooled.buffer(Long.BYTES * 3);
        long ledgerId = 1;
        long entry = 2;
        long lastConf = (long) (Math.random() * 10);
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);

        bookieImpl.setExplicitLac(buffer, new BookieImpl.NopWriteCallback(), "foo", "key".getBytes());
        //forziamo la scrittura della entry
        bookieImpl.forceLedger(ledgerId, new BookieImpl.NopWriteCallback(), "foo");
        ByteBuf readBuf = bookieImpl.getExplicitLac(ledgerId);
        assertEquals(ledgerId, readBuf.getLong(0));
        assertEquals(entry, readBuf.getLong(Long.BYTES));
        assertEquals(lastConf, readBuf.getLong(2*Long.BYTES));
    }

    @Test
    public void testEntryList() throws IOException, InterruptedException, BookieException {

        bookieImpl.ledgerStorage.flush(); //Siccome non abbiamo garanzie sull'ordine di esecuzione dei test, per prima cosa
        //eseguiamo il flush dei ledger, per "ripulire" e preparare l'ambiente al test successivo.

        long ledgerId = 2;
        Object ctx = "foo";

        for (int i = 0; i < 3; i++) {
            ByteBuf buffer = Unpooled.buffer(Long.BYTES * 3);

            long lastConf = (long) (Math.random() * 10);
            buffer.writeLong(ledgerId);
            buffer.writeLong(i);
            buffer.writeLong(lastConf);

            bookieImpl.addEntry(buffer, false, new BookieImpl.NopWriteCallback(), ctx, "key".getBytes());

        }
        //forziamo la scrittura della entry
        bookieImpl.forceLedger(ledgerId, new BookieImpl.NopWriteCallback(), "foo");
        PrimitiveIterator.OfLong iterator = bookieImpl.getListOfEntriesOfLedger(ledgerId);
        int i = 0;
        while (iterator.hasNext()) {
            assertEquals(i, iterator.nextLong());
            i++;
        }



    }


    private static File[] generateIndexDirs(int n) {
        File[] indexFiles = new File[n];
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            String dirPath = "./target/tempDirs/" + "index" + i + random.nextInt();
            File directory = new File(dirPath);
            directory.mkdir();
            indexFiles[i] = directory;
            dirs.add(indexFiles[i]);
        }

        return indexFiles;
    }

    private String[] generateTempDirs(int n, String suffix) {
        if (suffix.equals(LEDGER_STRING)) {
            ledgerFiles = new File[n];
        }
        String[] ret = new String[n];
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            String dirPath = "./target/tempDirs/" + suffix + i + random.nextInt();
            File directory = new File(dirPath);
            directory.mkdir();
            dirs.add(directory);
            ret[i] = directory.getPath();
            if (suffix.equals(LEDGER_STRING)) {
                ledgerFiles[i] = directory;
            }
        }

        return ret;
    }

    @AfterClass
    public static void teardown() throws InterruptedException {
        Thread shutdownThread = new Thread(() -> bookieImpl.shutdown(0));
        shutdownThread.start();
        /*
            Nella GitHub action a volte il metodo shutdown provoca un deadlock sul metodo shutdown, bloccando tutti i
            test successivi e facendo fallire la build dopo un timeout. Proviamo a fare lo shutdown del bookie nel thread
            e aspettare per un tempo limitato, dopodichè andare avanti per "saltare" l'eventualità in cui questo accada
         */
        shutdownThread.join(3000);
        shutdownThread.interrupt();
    }

}
