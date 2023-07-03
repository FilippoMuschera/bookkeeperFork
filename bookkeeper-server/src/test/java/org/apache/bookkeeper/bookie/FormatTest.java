package org.apache.bookkeeper.bookie;


import org.apache.bookkeeper.bookie.util.TestBookieImplUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.bookkeeper.bookie.util.FolderDeleter.deleteFolder;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.DataType.*;
import static org.apache.bookkeeper.bookie.util.TestBookieImplUtil.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(Parameterized.class)
public class FormatTest {

    public static List<File> folderList = new ArrayList<>();
    public static final String METADATA_PATH = "metadata-dir";
    public static final int NUM_OF_FOLDERS = 4;
    public static final String JOURNAL = "journal";
    public static final String LEDGER = "ledger";
    public static final String INDEX = "index";
    public static final String INVALID_PATH = "invalidPath\0";
    public static List<File> journalList = new ArrayList<>();
    public static List<File> ledgerList = new ArrayList<>();
    public static List<File> indexList = new ArrayList<>();
    public static int FOLDER_COUNT = 0;
    private String metaDataPath = METADATA_PATH;
    private final boolean force;
    private final boolean expectedOutput;
    private final boolean noExJournal;
    private final boolean noExLedg;
    private final boolean noExIndex;


    public FormatTest(TestBookieImplUtil.DataType journalDirs, TestBookieImplUtil.DataType ledgerDirs, TestBookieImplUtil.DataType indexDirs,
                      TestBookieImplUtil.DataType metadataPath, boolean force, boolean expected) {
        cleanAll();
        if (journalDirs == NULL)
            journalList = null;
        else if (journalDirs == INVALID)
            journalList.add(null);

        if (ledgerDirs == NULL)
            ledgerList = null;
        else if (ledgerDirs == INVALID)
            ledgerList.add(null);

        if (indexDirs == NULL)
            indexList = null;
        else if (indexDirs == INVALID)
            indexList.add(null);
        if (metadataPath == INVALID)
            this.metaDataPath = INVALID_PATH;
        else if (metadataPath == NULL)
            this.metaDataPath = null;
        else if (metadataPath == NOT_EX)
            this.metaDataPath = "not/a/real/path";

        this.force = force;
        this.expectedOutput = expected;

        //NOT_EX = Not Existing -> deve esserci un path che è valido, ma il relativo file non deve esistere
        noExJournal = journalDirs == NOT_EX;
        noExLedg = ledgerDirs == NOT_EX;
        noExIndex = indexDirs == NOT_EX;


    }

    @Parameterized.Parameters
    public static Collection<Object> getParams() {
        return Arrays.asList(new Object[][]{
                //journalDirs       //ledgerDirs        //indexDirs         //metadataPath      //force        //expected
                    {VALID,               VALID,              VALID,               VALID,         true,           true},
                    {NULL,                NULL,               NULL,                NULL,          true,           true},
                    {INVALID,             VALID,              VALID,               VALID,         true,           false},
                    {VALID,               INVALID,            VALID,               VALID,         true,           false},
                    {VALID,               VALID,              INVALID,             VALID,         true,           false},
                    {VALID,               VALID,              VALID,               INVALID,       true,           false},
                    {VALID,               VALID,              VALID,               VALID,         false,          false},
                    {NOT_EX,              NOT_EX,             NOT_EX,              NOT_EX,         true,           true}



        });

    }

    @Before
    public void before() {
        createDirs();

    }




    @AfterClass
    public static void after() {
        cleanAll();
        deleteFolder(new File("./not")); //elimino "/not/a/real/path/"
        deleteFolder(new File(METADATA_PATH));

    }



    @Test
    public void formatTest() {
        ServerConfiguration configuration = new ServerConfiguration();
        String[] journalStrings = journalList == null ? null : extractFileNames(journalList);
        if (noExJournal)
            journalStrings[0] = "not/a/real/path"; //se journalStrings è null noExJournal = false -> non rischio NullPointerException
        configuration.setJournalDirsName(journalStrings);
        String[] ledgStrings = ledgerList == null ? null : extractFileNames(ledgerList);
        if (noExLedg)
            ledgStrings[0] = "not/a/real/path";
        configuration.setLedgerDirNames(ledgStrings);

        String[] indexStrings = indexList == null ? null : extractFileNames(indexList);
        if (noExIndex)
            indexStrings[0] = "not/a/real/path";
        configuration.setIndexDirName(indexStrings);
        configuration.setGcEntryLogMetadataCachePath(this.metaDataPath);
        boolean output = BookieImpl.format(configuration, false, this.force);
        assertEquals(expectedOutput, output);
    }





}
