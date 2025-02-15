package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.util.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(Parameterized.class)
public class BookieImplMountLedgerTest {

    private LedgerStorage ledgerStorage;
    private ServerConfiguration serverConfiguration;
    private ExpectedValue expectedValue;
    private ExpectedValue actualValue;

    @Parameterized.Parameters
    public static Collection<Object[]> getParams() {
        return Arrays.asList(new Object[][]{

                //LedgerStorage        UsageThreshold         WarnThreshold       ExpectedValue
         {LedgerStorageType.SORTED,         -1.0f,                 -1.1f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.SORTED,         -1.0f,                 -1.0f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.SORTED,         -1.0f,                  0.0f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.SORTED,          0.0f,                 -1.0f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.SORTED,          0.0f,                  0.0f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.SORTED,          0.0f,                  1.0f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.SORTED,          0.1f,                  0.0f,        ExpectedValue.PASSED},
         {LedgerStorageType.INTERLEAVED,     0.1f,                  0.1f,        ExpectedValue.PASSED},
         {LedgerStorageType.INTERLEAVED,     0.1f,                  0.2f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.INTERLEAVED,     1.0f,                  0.9f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.INTERLEAVED,     1.0f,                  1.0f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.INTERLEAVED,     1.0f,                  1.1f,        ExpectedValue.ILLEGAL_ARGUMENT},
         {LedgerStorageType.DB,              0.9f,                  0.8f,        ExpectedValue.PASSED},
         {LedgerStorageType.NULL,            0.9f,                  0.8f,        ExpectedValue.PASSED},






        });
    }

    public BookieImplMountLedgerTest(LedgerStorageType type, float usageThreshold, float warnThreshold, ExpectedValue expectedValue) {

        this.ledgerStorage = this.getRequestedLedger(type);
        this.serverConfiguration = TestBKConfiguration.newServerConfiguration();
        serverConfiguration.setDiskUsageThreshold(usageThreshold);
        serverConfiguration.setDiskUsageWarnThreshold(warnThreshold);
        this.expectedValue = expectedValue;
    }

    private LedgerStorage getRequestedLedger(LedgerStorageType type) {
        String ls = "";
        LedgerStorage ledgerStorage = null;
        switch (type) {
            case INTERLEAVED:
                ls = "org.apache.bookkeeper.bookie.InterleavedLedgerStorage";
                break;
            case SORTED:
                ls = "org.apache.bookkeeper.bookie.SortedLedgerStorage";
                break;
            case DB:
                ls = "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage";
                break;
            case NULL:
                return null;


        }
        try {
            ledgerStorage = LedgerStorageFactory.createLedgerStorage(ls);
            if (type == LedgerStorageType.INTERLEAVED) { //Il ledger DB non usa i checkpoint
                ledgerStorage.setCheckpointer(new Checkpointer() {
                    @Override
                    public void startCheckpoint(CheckpointSource.Checkpoint checkpoint) {
                        throw new RuntimeException();
                    }

                    @Override
                    public void start() {
                        throw new RuntimeException();

                    }
                });
            }


        } catch (IOException e) {
            System.err.println("Impossibile istanziare il ledger di tipo " + type + " con la stringa " + ls);
            e.printStackTrace();
        }

        return ledgerStorage;

    }

    @Test
    public void mountLedgerTest() {

        actualValue = ExpectedValue.PASSED;

        try {
            LedgerStorage ls = BookieImpl.mountLedgerStorageOffline(serverConfiguration, ledgerStorage);
            //Dopo aver visto i risultati di PIT aggiungiamo ulteriori controlli
            //sul valore di ritorno del metodo, per aumentare il numero di mutazioni KILLED
            //e quindi la qualità del test stesso
            assertEquals(0, ls.localConsistencyCheck(java.util.Optional.empty()).size());
            ls.checkpoint(CheckpointSource.Checkpoint.MIN);
            if (ls instanceof InterleavedLedgerStorage) {
                InterleavedLedgerStorage ils = (InterleavedLedgerStorage) ls;
                ils.onRotateEntryLog();

            }
        } catch (IllegalArgumentException e) {
            actualValue = ExpectedValue.ILLEGAL_ARGUMENT;

        } catch (IOException e) {
            System.err.println("IOException verificatasi durante l'esecuzione di mountLedgerTest()");
        }

        Assert.assertEquals(expectedValue, actualValue);


    }

    private enum LedgerStorageType {
        INTERLEAVED, SORTED, DB, NULL
    }

    private enum ExpectedValue {
        PASSED, ILLEGAL_ARGUMENT
    }
}
