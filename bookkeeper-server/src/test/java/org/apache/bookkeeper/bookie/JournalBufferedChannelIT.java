package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

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
    private Journal journal;
    private int capacity;
    JournalChannel journalChannel;
    BufferedChannel bufferedChannel;

    @Before
    public void setUp() throws IOException {
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

        journal = new Journal(
                1,
                directory,
                conf,
                ledgerDirsManager,
                new NullStatsLogger(),
                ByteBufAllocator.DEFAULT
        );

        journal.start(); //facciamo partire il journal
        capacity = 3 * Long.BYTES + stringForBuffer.getBytes().length;
        //Istanziamo il JournalChannel per il Journal che abbiamo appena creato
        journalChannel = new JournalChannel(journal.getJournalDirectory(), journal.getId());
        //Dal JournalChannel otteniamo il BufferedChannel, di cui vogliamo testare l'interazione con il Journal
        bufferedChannel = journalChannel.getBufferedChannel();

    }

    @Test
    public void journalWriterTest() throws Exception {



        //Creiamo i vari buffer che poi andremo a scrivere sul journal tramite il BufferedChannel presente nel
        //JournalChannel
        ByteBuf paddingBuff = Unpooled.buffer(2 * JournalChannel.SECTOR_SIZE); //buffer per il padding
        paddingBuff.writeZero(2 * JournalChannel.SECTOR_SIZE); //poniamo tutti i byte del buffer di padding a zero
        ByteBuf buffer = Unpooled.buffer(capacity); //buffer su cui scrivere i dati veri e propri
        ByteBuffer lenBuf = ByteBuffer.allocate(Integer.BYTES); //Alloco lo spazio per un solo int, tanto ci dovrò scrivere solo le lunghezze degli altri buff

        //Scrivo i dati sul buffer
        buffer.writeLong(ledgerId);
        buffer.writeLong(entry);
        buffer.writeLong(lastConf);
        buffer.writeBytes(stringForBuffer.getBytes());


        //Ora procediamo a scrivere i buffer sul BufferedChannel
        /*
        Vogliamo che la nostra entry sul journal abbia la struttura del tipo:

        +-----------------+-----------------------+------------------+
        |    Metadata     |         Dati          |     Padding      |
        +-----------------+-----------------------+------------------+

        Per simulare la scrittura di una entry sul Journal. I byte di Padding servono ad allineare la posizione su
        cui inizierà la entry successiva del Journal

         */

        //prima il buffer con i metadata
        ByteBuf metaEntry = ApacheBookieJournalUtil.generateMetaEntry(1, "test".getBytes());
        lenBuf.putInt(metaEntry.readableBytes());
        lenBuf.flip();
        bufferedChannel.write(Unpooled.wrappedBuffer(lenBuf));
        bufferedChannel.write(metaEntry);


        //ora il buffer con i Dati
        lenBuf.clear();
        lenBuf.putInt(buffer.readableBytes());
        lenBuf.flip();
        bufferedChannel.write(Unpooled.wrappedBuffer(lenBuf));
        bufferedChannel.write(buffer);

        //Ora il padding
        Journal.writePaddingBytes(journalChannel, paddingBuff, JournalChannel.SECTOR_SIZE);

        //ora flushiamo il BufferedChannel in modo che i dati vengano scritti sul Journal
        bufferedChannel.flushAndForceWrite(true);

        try {
            journal.scanJournal(journal.getId(), 0, new EchoScanner(metaEntry.readableBytes()));

        } catch (RuntimeException e) {
            e.printStackTrace();
            fail(e.getMessage()); //I bytes di metadata non corrispondono
        }

        /*Sfruttando il JournalScanner dichiarato poche righe più in basso (dopo aver skippato i metadati),
        * procediamo a leggere dal buffer 3 Long e una Stringa.
        * Per assicurarci che l'interazione tra i componenti di questo Integration test (Journal e BufferedChannel, con
        * "l'intermezzo" del JournalChannel che ci permette di ottenere il BufferedChannel), andiamo ora a controllare
        * che quello che abbiamo scritto sul journal tramite il BufferedChannel, sia esattamente quello che poi ritroviamo
        * sul Journal stesso quando invochiamo la journal.scanJournal(..).
        * Il test chiaramente è passato solo se scrittura e lettura dei dati corrispondono, e quindi se lo scambio di dati
        * tra il journal e il buffered channel è avvenuto con successo.
        * Non abbiamo testato, in questa fase, il corretto funzionamento in isolamento del Journal e del BufferedChannel,
        * perchè essendo un test d'integrazione, ci siamo concentrati sull'acquisire confidenza sulla loro interazione,
        * assumendo che i test di Unità fossero già stati superati per entrambe le classi. */

        assertEquals(ledgerId, firstLong);
        assertEquals(entry, secondLong);
        assertEquals(lastConf, lastLong);
        assertEquals(stringForBuffer, readString);
        /*
         Questo assert serve a controllare che il BufferedChannel sia effettivamente pronto alla prossima scrittura sul
         Journal, perchè abbiamo richiesto, tramite Journal.writePaddingBytes(...) che venissero inseriti dei byte di
         padding tra quello che abbiamo scritto sul file del Journal, e l'inizio della prossima scrittura. Quindi il
         controllo è volto a testare che la richiesta dell'inserimento dei padding bytes da parte del Journal è stata
         correttamente svolta dal BufferedChannel, e che quindi lo scambio di messaggi tra le due istanze delle classi
          sia andato a buon fine.
         */
        assertEquals(0, journalChannel.bc.position % JournalChannel.SECTOR_SIZE);



    }

    @After
    public void cleanUp() throws IOException {
        journalChannel.close();
        journal.shutdown();

    }

    private static class EchoScanner implements Journal.JournalScanner {

        private static boolean metaData = true; //Lo scanner verrà invocato prima per leggere il segmento di metadata
        //e poi quello dei Dati. Questo boolean serve semplicemente a distinguere i due casi.
        private final long metadataSize;

        public EchoScanner(long metadataSize) {
            this.metadataSize = metadataSize;
        }

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            if (!metaData){
                firstLong = entry.getLong();
                secondLong = entry.getLong();
                lastLong = entry.getLong();
                byte[] stringBytes = new byte[entry.remaining()];
                entry.get(stringBytes);
                readString = new String(stringBytes, StandardCharsets.UTF_8);

                // Echo dei dati a schermo
                System.out.println("First long: " + firstLong);
                System.out.println("Second long: " + secondLong);
                System.out.println("Last long: " + lastLong);
                System.out.println("String: " + readString);
            }
            else {
                long remaining = entry.remaining();
                if (remaining != this.metadataSize)
                    throw new RuntimeException("La lunghezza dei byte di metadata non corrisponde!" +
                            "Expected: " + this.metadataSize + ", Actual: " + remaining);
            }
            metaData = false;


        }
    }
}
