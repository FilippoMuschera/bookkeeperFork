package org.apache.bookkeeper.bookie;


import io.netty.buffer.*;
import org.apache.bookkeeper.bookie.util.TestBookieImplUtil;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidLedgerDirsManager;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidServerConfig;
import org.apache.bookkeeper.bookie.util.testtypes.InvalidStatsLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.ExpectedValue.*;
import static org.junit.jupiter.api.Assertions.*;

@RunWith(Parameterized.class)
public class SetUpJournalTest {

    public static volatile boolean uncaughtExceptionInThread = false;
    public static volatile Throwable threadException = null;
    private static boolean busyWait = true;
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    Journal journal;
    int index;
    ServerConfiguration config;
    LedgerDirsManager ledgerDirsManager;
    StatsLogger statsLogger;
    ByteBufAllocator byteBufAllocator;
    TestBookieImplUtil.ExpectedValue expectedValue;
    File directory;

    public SetUpJournalTest(int index, Type dirType, Type configType, Type ledgDirManType, StatsLogger logger, ByteBufAllocator bba, TestBookieImplUtil.ExpectedValue expected) throws IOException {
        this.index = index;
        this.directory = getDirType(dirType);
        this.config = getConfigType(configType, this.directory);
        this.ledgerDirsManager = getLedgerDirsManagerType(ledgDirManType, this.config);
        this.statsLogger = logger;
        this.byteBufAllocator = bba;
        this.expectedValue = expected;
        uncaughtExceptionInThread = false;
        threadException = null;


    }

    @Parameterized.Parameters
    public static Collection<Object> getParams() {
        return Arrays.asList(new Object[][]{
                //index //directory //config //ledgDirsManager   //statsLog         //byteBufAlloc              //Expected
                {-1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED}, //busy wait enabled, no flushOnEmptyQueue
                {-1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED}, //busy wait disabled, flushOnEmptyQueue enabled from here on
                {0, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, PASSED},
                {1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), PooledByteBufAllocator.DEFAULT, PASSED},
                {1, Type.INVALID, Type.VALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, IO_EXCEPTION},
                {1, Type.VALID, Type.INVALID, Type.VALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, RE_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.INVALID, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, LEDG_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.VALID, null, UnpooledByteBufAllocator.DEFAULT, NP_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.VALID, new InvalidStatsLogger(), UnpooledByteBufAllocator.DEFAULT, RE_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), null, BUFF_EXCEPTION},
                {1, Type.VALID, Type.VALID, Type.VALID, new NullStatsLogger(), new InvalidBBA(), RUNTIME_BUFF_EXCEPTION},
                {1, Type.VALID, Type.VALID, null, new NullStatsLogger(), UnpooledByteBufAllocator.DEFAULT, NP_EXCEPTION},



        });

    }


    public static LedgerDirsManager getLedgerDirsManagerType(Type ledgDirManType, ServerConfiguration conf) throws IOException {
        if (ledgDirManType == null) return null;
        else if (ledgDirManType == Type.VALID) {
            return new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(0.99f, 0.98f));
        } else if (ledgDirManType == Type.INVALID) {
            return new InvalidLedgerDirsManager(true);
            /*
             * In questo caso scegliamo un InvalidLedgerDirsManager che fa il throw di una unchecked exception quando
             * viene chiamato uno dei suoi metodi, perchè altrimenti, ritornando semplicemente un valore scorretto per
             * la lista di LedgerDirs (uno dei path ha il carattere '\0', non consentito nel path di un File) si genera un
             * errore nella configurazione del Journal, ma l'errore è visibile solo a livello del LOG, dove compare il messaggio
             * "Problems reading from nome_file (this is okay if it is the first time starting this bookie)".
             * Non lo definirei propriamente un Bug perchè il metodo sembra effettivamente pensato per comportarsi così, anche
             * se è un comportamento poco chiaro, dato che comunque un errore viene generato, ma non viene realmente gestito
             * (viene solo scritto nel LOG che c'è stato un errore, non c'è nessuna logica per la gestione dell'errore),
             * e inoltre il messaggio di errore è "ingannevole", in quanto viene generato anche la prima volta che sia avvia
             * il Bookie a cui è associato quel Journal, e quindi non è sempre immediato capire se il messaggio d'errore
             * è dovuto realmente a un errore oppure al primo avvio del Bookie. Sarebbe forse quantomeno opportuno differenziare
             * il messaggio per l'errore vero o per il primo avvio del Bookie, se non si ritiene necessario lanciare un eccezione
             * verso l'esterno.
             */

        }
        return null;
    }

    public static ServerConfiguration getConfigType(Type configType, File dir) throws IOException {

        if (configType == null) return null;
        else if (configType == Type.VALID) {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setJournalDirsName(new String[]{dir.getPath()})
                    .setMetadataServiceUri(null)
                    .setJournalAdaptiveGroupWrites(false)
                    .setMaxBackupJournals(0)
                    .setBusyWaitEnabled(busyWait)
                    .setJournalFlushWhenQueueEmpty(!busyWait) //stesso discorso di busyWait, per semplicità uso una sola variabile
                    .setBookiePort(1);
            busyWait = false; //lo userò solo per la prima run, avevamo un missed branch identificato grazie a Jacoco
            File directory = temporaryFolder.newFolder();
            BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(directory));
            conf.setLedgerDirNames(directory.list());
            return conf;
        } else if (configType == Type.INVALID) {
            return new InvalidServerConfig();
        }

        fail(); //non dovrei mai arrivare qui
        return null;

    }

    public static File getDirType(Type dirType) throws IOException {
        Random random = new Random();
        if (dirType == null) return null;
        else if (dirType == Type.VALID) {

            File directory = temporaryFolder.newFolder();
            BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(directory));
            return directory;
        } else if (dirType == Type.INVALID) {
            String path = "/target/tampDirs/journal" + random.nextInt() + "\0"; // TODO se ho tempo qui devo mettere una cartella temporanea
            //Non c'è (più) un @After che cancella questa cartella perchè per qualche motivo (la documentazione di PIT non
            //è moto chiara a riguardo) quando PIT esegue questi test nella sua esecuzione, fallisce, nonostante questo test vada a buon fine
            //sia con mvn test che con mvn verify, sia con il profilo di Jacoco che con quello di Ba-dua
            return new File(path);
        }
        return null;
    }


    @Test
    public void newJournalTest() throws IOException {
        Exception exceptionRaised = null;

        try {
            this.journal = new Journal(
                    this.index,
                    this.directory,
                    this.config,
                    this.ledgerDirsManager,
                    this.statsLogger,
                    this.byteBufAllocator
            ) {
                @Override
                protected void handleException(Thread t, Throwable e) {
                    /*
                     * Il metodo originale chiama la exit() del processo, dunque quando abbiamo una configurazione del
                     * Journal che porta all'esecuzione di questo handler, il test viene interrotto e fallisce. Facciamo
                     * dunque l'override del metodo per poter eseguire il test in maniera "controllata", senza modificare
                     * realmente la logica del Journal, dato che con l'invocazione di questo metodo verrebbe semplicemente arrestato.
                     * Procediamo infatti a controllare che l'invocazione dell'handler sia avvenuta, controlliamo che il
                     * tipo di eccezione che ne ha reso necessaria l'esecuzione sia quello che ci aspettavamo, dopodiché
                     * il test termina, provocando la chiusura del journal nell' @After se ancora in esecuzione, come
                     * vorrebbe la logica dell'handler originale.
                     */
                    System.out.println("There would have been an uncaught exception in the Journal, but I caught it!");
                    System.out.println(e.toString());
                    uncaughtExceptionInThread = true;
                    threadException = e;
                }
            };


            journal.start();
            /*
             * Non è un modo molto elegante di aspettare che il journal parta ma è necessario per evitare di utilizzare
             * il journal mentre sta ancora eseguendo la start. Anche gli sviluppatori Apache implementano (seppur con
             * un meccanismo più complesso) la wait dell'esecuzione dell'avvio del journal, e in alcune occasioni fanno
             * uso direttamente della sleep(...) nei loro test, perciò, anche se è una soluzione sicuramente da migliorare,
             * per questioni di tempo viene lasciata in questo modo.
             */
            Thread.sleep(500);


        } catch (Exception e) {
            exceptionRaised = e;
            assertFalse(uncaughtExceptionInThread);
        }

        if (this.expectedValue == IA_EXCEPTION) {
            assertTrue(exceptionRaised instanceof IllegalArgumentException);
        } else if (this.expectedValue == RE_EXCEPTION) {
            assertTrue(exceptionRaised instanceof RuntimeException);

        } else if (expectedValue == LEDG_EXCEPTION) {
            assertTrue(exceptionRaised instanceof ArrayIndexOutOfBoundsException);
        } else if (expectedValue == NP_EXCEPTION) {
            assertTrue(exceptionRaised instanceof NullPointerException);
        } else if (expectedValue == BUFF_EXCEPTION) {
                /*
                 In questo caso sfruttiamo l'override dell'exception handler per controllare che il journal sarebbe in
                 realtà uscito dalla sua esecuzione (il che però avrebbe interrotto il test, da qui la necessità dell'override).
                 Ci assicuriamo dunque tramite l'assert che l'handler sia stato eseguito.
                 */

            assertTrue(uncaughtExceptionInThread);
            assertTrue(threadException instanceof NullPointerException);


        } else if (expectedValue == RUNTIME_BUFF_EXCEPTION) {
            assertTrue(uncaughtExceptionInThread);
            assertTrue(threadException instanceof IndexOutOfBoundsException);
        }
        //Casi in cui la creazione del Journal non solleva direttamente un'eccezione

        else {
            if (expectedValue == IO_EXCEPTION) {
                //Se c'è stata un eccezione IOException interna, voglio vedere che il journal ha interrotto
                //il suo thread, e che la creazione della sua cartella non è andata a buon fine
                assertFalse(journal.journalDirectory.canRead() || journal.journalDirectory.canWrite() || journal.journalDirectory.exists());
                return;
            }

           /* Se invece la configurazione di parametri usata per la creazione del journal va a buon fine, eseguiamo una
            * serie di controlli per verificare che il Journal sia coerente con quanto "stabilito" dai parametri passati
            * al suo costruttore, e controlliamo che reagisca correttamente alle chiamate ai metodi della sua classe.
            * Nello specifico controlliamo che:
            * 1. Non ci siano state eccezioni durante la sua creazione
            * 2. La journalDirectory è impostata correttamente.
            * 3. Il Journal è in esecuzione
            * 4. La sua cartella abbia i corretti permessi di lettura e scrittura
            * 5. La size massima del Journal sia stata settata correttamente (abbiamo usato il default: 2 MB)
            * 6. La memoria che risulta attualmente utilizzata non sia superiore di quella totale disponibile
            * 7. Testiamo il corretto funzionamento del meccanismo de checkpoint del Journal

             */

            assertNull(exceptionRaised);
            assertEquals(this.directory, journal.getJournalDirectory());
            assertTrue(journal.running);
            assertTrue(journal.journalDirectory.canRead() && journal.journalDirectory.canWrite() && journal.journalDirectory.exists());
            //Il Journal prende in input la dimensione in MB, ma la conserva in Bytes. Piccolo controllo sulla conversione.
            assertEquals(config.getMaxJournalSizeMB() * 1024 * 1024, journal.maxJournalSize);
            assertTrue(journal.getJournalStats().getJournalMemoryMaxStats().getDefaultValue() >= journal.getMemoryUsage());

            /*
             * Ora testiamo la funzionalità del nostro Journal per aggiungere una log entry, tramite il metodo logAddEntry().
             * Quello che ci aspettiamo è un esecuzione che non sollevi eccezioni, una scan Journal che riesca a fare lo scan anche
             * del nuovo file di log senza problemi ( = senza eccezioni), e poi più in basso, dopo aver testato anche i
             * checkpoint, controlleremo che il file di log qui creato venga correttamente letto anche dal meccanismo di
             * checkpoint del journal.
             */

            CountDownLatch latch = new CountDownLatch(1); //è un semaforo sostanzialmente
            ByteBuf byteBuf = Unpooled.buffer(512);
            byteBuf.writeLong(1); //ledgerID
            byteBuf.writeLong(2); //entryId

            try {

                journal.logAddEntry(byteBuf, false, (rc, ledgerId1, entryId1, addr, ctx) -> latch.countDown(), "foo");

                //aspettiamo che la addEntry abbiamo finito, perchè la callBack diminuisce di 1 il latch, facendolo arrivare a 0
                //Usiamo il latch con un tempo limite in modo da uccidere le mutazioni di PIT che altrimenti verrebbero solamente segnate
                //con TIMED_OUT, poichè chiaramente, se la mutazione interessa la addEntry, il latch.countDown() non viene mai
                //eseguito, il test rimane bloccato a questa riga.
                assertTrue(latch.await(20, TimeUnit.SECONDS));

                //ora lanciamo la scan journal per controllare che non trovi record corrotti, per verificare che l'esecuzione sia andata
                //a buon fine
                journal.scanJournal(journal.getId(), 0, (journalVersion, offset, entry) -> {
                    //dummy
                });
            } catch (Exception e) {
                fail("Exception while adding a log entry, and there shouldn't have been one!");
                e.printStackTrace();
            }


            /*
             * Per testare i checkpoint, ci poniamo nella situazione in cui ci siano 3 file di log del journal all'interno
             * della sua cartella (file con estensione .txn). 1 è già presente, viene creato al momento della creazione del
             * Journal, gli altri li aggiungiamo manualmente. In questa fase, infatti, non ci interessa realmente del loro
             * contenuto, ma ci interessa solamente la loro presenza (e il loro nome, vedi dopo), e dunque non è un problema
             * crearli manualmente.
             * Il sistema dei checkpoint funziona con gli ID ( = nomi) dei file di log del journal. Se su un Journal si dichiara che
             * c'è un checkpoint con checkpointID = x, e che questo checkpoint è stato completato, significa che tutti i dati presenti
             * nei file con ID <= checkpointID non servono più, perchè sono stati flushati sullo strato di
             * persistenza, e quindi i file che rispettano questa condizione possono essere eliminati tramite il
             * meccanismo di garbage collection di BookKeeper.
             * L'idea quindi è quella di usare il momento della creazione del file di log del journal come nome del file
             * stesso, e quindi poi creare dei checkpoint con un ID superiore al momento di creazione di quei file.
             *
             * Quello che facciamo quindi è aggiungere ulteriori file alla journalDirectory, e poi creare un checkpoint
             * successivo a tutti i file attualmente presenti nella journalDirectory, "fingendo" di averli processati.
             * Quando andiamo a completare questo checkpoint ci aspettiamo che tutti i file .txn vengano eliminati dal
             * Journal, dato che devono risultare con un ID minore di quello del checkpoint.
             */

            int n = 2;
            for (int i = 0; i < n; i++) {
                File dir = config.getJournalDirs()[0];

                //il +i serve a essere assolutamente certi che per quanto l'esecuzione possa essere veloce, i due file
                //avranno un nome diverso.
                File newFile = new File(dir.getPath(), Long.toHexString(System.currentTimeMillis() + i) + ".txn");
                assertTrue(newFile.createNewFile());
            }
            //n li ho aggiunti manualmente, uno viene generato al momento della creazione del Journal e l'altro viene
            //generato tramite il test che usa la logAddEntry eseguita sopra (in questo modo controlliamo ulteriormente
            //il fatto che la logAddEntry sia andata a buon fine e abbia creato un file di log)
            assertEquals(2 + n, Journal.listJournalIds(config.getJournalDirs()[0], null).size());

            //2*currentTime per assicurarci che il checkpoint sia successivo alla creazione dei file del ciclo for
            //l'epoch moltiplicata per due cade circa nel 2076 quindi sembra un margine più che sufficiente :)
            journal.setLastLogMark(2 * System.currentTimeMillis(), 0);


            try {
                journal.checkpointComplete(journal.newCheckpoint(), true);

            } catch (Exception e) {
                fail("No exception expected here");
            }

            /*
             * L'assert ha un expected = 1 e non = 0 perchè in realtà al momento della creazione del Journal
             * al suo interno viene creata anche una cartella "/current/", che quindi è l'elemento che risulterà
             * nel listFiles(). Se ci fosse ancora qualche file .txn la listFiles troverebbe un numero di file
             * > 1, e quindi l'assert fallirebbe comunque.
             */
            assertEquals(1, Objects.requireNonNull(journal.getJournalDirectory().listFiles()).length);

            //Controlliamo anche che, chi ha superato tutti questi test, lo dovesse effettivamente fare
            assertEquals(this.expectedValue, PASSED);

            journal.shutdown();

        }


    }

    private enum Type {
        VALID, INVALID
    }

    private static class InvalidBBA extends AbstractByteBufAllocator {

        @Override
        public ByteBuf directBuffer(int i) {

            //Istanza invalida che fa il throw di una unchecked exception
            throw new IndexOutOfBoundsException();

        }

        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }

        @Override
        protected ByteBuf newHeapBuffer(int i, int i1) {
            return null;
        }

        @Override
        protected ByteBuf newDirectBuffer(int i, int i1) {
            return null;
        }

    }


    @After
    public void after() {
        if (journal != null && journal.running)
            journal.shutdown();
    }

}
