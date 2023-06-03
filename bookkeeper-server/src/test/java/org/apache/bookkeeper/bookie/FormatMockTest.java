package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.apache.bookkeeper.bookie.FormatTest.*;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.*;
import static org.junit.Assert.*;

@PrepareForTest({IOUtils.class, BookieImpl.class})
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class FormatMockTest {


    @Before
    public void cleanBefore(){cleanAll();}

    @After
    public void cleanAfter(){cleanAll();}


    @Test
    public void formatTestWithMockTrue() throws Exception {

        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.when(IOUtils.confirmPrompt("Are you sure to format Bookie data..?")).thenAnswer((Answer<Boolean>) invocation -> true);

        cleanAll();
        createDirs();

        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setJournalDirsName(extractFileNames(journalList));
        configuration.setLedgerDirNames(extractFileNames(ledgerList));
        configuration.setIndexDirName(extractFileNames(indexList));
        configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);
        boolean output = BookieImpl.format(configuration, true, false);
        assertTrue(output);



    }

    @Test
    public void formatTestWithMockFalse() throws Exception {

        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.when(IOUtils.confirmPrompt("Are you sure to format Bookie data..?")).thenAnswer((Answer<Boolean>) invocation -> false);

        cleanAll();
        createDirs();

        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setJournalDirsName(extractFileNames(journalList));
        configuration.setLedgerDirNames(extractFileNames(ledgerList));
        configuration.setIndexDirName(extractFileNames(indexList));
        configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);
        boolean output = BookieImpl.format(configuration, true, false);
        assertFalse(output);


    }

    @Test
    public void formatTestWithMockException() throws Exception {

        /*
         In questo test, come si vede dal mock, viene lanciata una IOException (la lanciamo da IOUtils perchè è un oggetto
         di BookKeeper, anche se lo scenario in cui si potrebbe verificare realmente è quello in cui questa eccezione è
         lanciata da System.in.read(), che però non essendo un tipo che "possediamo", evitiamo di mockare.
         BookieImpl.format() provvede a fare il catch dell'eccezione e ritornare semplicemente false.
         Come si vede infatti dalla segnatura di format(), non esegue il throws di nessuna eccezione.
         */

        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.when(IOUtils.confirmPrompt("Are you sure to format Bookie data..?")).thenThrow(IOException.class);

        cleanAll();
        createDirs();

        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setJournalDirsName(extractFileNames(journalList));
        configuration.setLedgerDirNames(extractFileNames(ledgerList));
        configuration.setIndexDirName(extractFileNames(indexList));
        configuration.setGcEntryLogMetadataCachePath(METADATA_PATH);
        boolean output = BookieImpl.format(configuration, true, false);
        assertFalse(output);




    }


}
