package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JournalBufferedChannelIT {


    //Da Scrivere
    long ledgerId = 1;
    long entry = 0;
    long lastConf = (long) (Math.random() * 10);
    String stringForBuffer = "someString";


    //Da leggere
    static long firstLong;
    static long secondLong;
    static long lastLong;
    static String readString;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void journalWriterTest() throws Exception {
        //Oggetti per istanzaire il Journal
        File directory = temporaryFolder.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(directory));
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalDirsName(new String[]{directory.getPath()})
                .setMetadataServiceUri(null)
                .setJournalAdaptiveGroupWrites(false)
                .setMaxBackupJournals(0)
                .setBusyWaitEnabled(false)
                .setJournalFlushWhenQueueEmpty(false)
                .setFlushInterval(20000)
                .setBookiePort(1);

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(0.99f, 0.98f));

        Journal journal = new Journal(
                1,
                directory,
                conf,
                ledgerDirsManager,
                new NullStatsLogger(),
                ByteBufAllocator.DEFAULT
        );

        journal.start(); //facciamo partire il journal

        int capacity = 65536;
        //Istanziamo il JournalChannel per il Journal che abbiamo appena creato
        JournalChannel journalChannel = new JournalChannel(journal.getJournalDirectory(), journal.getId());

        //Creiamo i vari buffer che poi andremo a scrivere sul journal tramite il BufferedChannel presente nel
        //JournalChannel
        ByteBuf paddingBuff = Unpooled.buffer(capacity);
        paddingBuff.writeZero(2 * JournalChannel.SECTOR_SIZE);
        ByteBuf buffer = Unpooled.buffer(capacity);



        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        buffer.writeBytes(stringForBuffer.getBytes());

        ByteBuffer lenBuf = ByteBuffer.allocate(4);

        BufferedChannel bufferedChannel = journalChannel.getBufferedChannel();

        //Ora procediamo a scrivere i buffer sul BufferedChannel

        //prima le meta entry
        ByteBuf metaEntry = ApacheBookieJournalUtil.generateMetaEntry(1, "test".getBytes());
        lenBuf.putInt(metaEntry.readableBytes());
        lenBuf.flip();
        bufferedChannel.write(Unpooled.wrappedBuffer(lenBuf));
        bufferedChannel.write(metaEntry);

        //ora il pacchetto vero e proprio
        lenBuf.clear();
        lenBuf.putInt(buffer.readableBytes());
        lenBuf.flip();
        bufferedChannel.write(Unpooled.wrappedBuffer(lenBuf));
        bufferedChannel.write(buffer);

        //Ora il padding
        Journal.writePaddingBytes(journalChannel, paddingBuff, JournalChannel.SECTOR_SIZE);

        //ora flushiamo il BufferedChannel in modo che i dati vengano scritti sul Journal
        bufferedChannel.flushAndForceWrite(false);

        journal.scanJournal(journal.getId(), 0, new EchoScanner());

        /*Sfruttando il JournalScanner dichiarato poche righe più in basso (dopo aver skippato i metadati),
        * procediamo a leggere dal buffer 3 Long e una Stringa.
        * Per assicurarci che l'interazione tra i componenti di questo Integration test (Journal e BufferedChannel, con
        * "l'intermezzo" del JournalChannel che ci permette di ottenere il BufferedChannel), andiamo ora a controllare
        * che quello che abbiamo scritto sul journal tramite il BufferedChannel, sia esattamente quello che poi ritroviamo
        * sul Journal stesso quando invochiamo la journal.scanJournal.
        * Il test chiaramente è passato solo che scrittura e lettura dei dati corrispondono, e quindi se lo scambio di dati
        * tra il journal e il buffered channel è avvenuto con successo.
        * Non abbiamo testato, in questa fase, il corretto funzionamento in isolamento del Journal e del BufferedChannel,
        * perche essendo un test d'integrazione, ci siamo concentrati sull'acquisire confidenza sulla loro interazione,
        * assumendo che i test di Unità fossero già stati superati per entrambe le classi. */

        assertEquals(ledgerId, firstLong);        assertEquals(entry, secondLong);
        assertEquals(lastConf, lastLong);
        assertEquals(stringForBuffer, readString);



    }

    private static class EchoScanner implements Journal.JournalScanner {

        private static boolean skipMetadata = true;
        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            if (!skipMetadata){
                firstLong = entry.getLong();
                secondLong = entry.getLong();
                lastLong = entry.getLong();
                byte[] stringBytes = new byte[entry.remaining()];
                entry.get(stringBytes);
                readString = new String(stringBytes, StandardCharsets.UTF_8);

                // Stampa dei dati a schermo
                System.out.println("First long: " + firstLong);
                System.out.println("Second long: " + secondLong);
                System.out.println("Last long: " + lastLong);
                System.out.println("String: " + readString);
            }
            skipMetadata = false;

        }
    }
}
