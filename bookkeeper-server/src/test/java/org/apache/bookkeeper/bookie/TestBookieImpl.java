//IT ORIGINALLY WAS AN APACHE-BOOKKEEPER TEST CLASS
package org.apache.bookkeeper.bookie;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.proto.SimpleBookieServiceInfoProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test wrapper for BookieImpl that chooses defaults for dependencies.
 */
public class TestBookieImpl extends BookieImpl {
    private static final Logger log = LoggerFactory.getLogger(TestBookieImpl.class);

    private final Resources resources;

    public TestBookieImpl(ServerConfiguration conf) throws Exception {
        this(new ResourceBuilder(conf).build());
    }

    public TestBookieImpl(Resources resources) throws Exception {
        super(resources.conf,
                resources.registrationManager,
                resources.storage,
                resources.diskChecker,
                resources.ledgerDirsManager,
                resources.indexDirsManager,
                NullStatsLogger.INSTANCE,
                UnpooledByteBufAllocator.DEFAULT,
                new SimpleBookieServiceInfoProvider(resources.conf));
        this.resources = resources;
    }

    @Override
    int shutdown(int exitCode) {
        int ret = super.shutdown(exitCode);
        resources.cleanup();
        return ret;
    }

    /**
     * Manages bookie resources including their cleanup.
     */
    public static class Resources {
        private final ServerConfiguration conf;
        private final MetadataBookieDriver metadataDriver;
        private final  RegistrationManager registrationManager;
        private final LedgerManagerFactory ledgerManagerFactory;
        private final LedgerManager ledgerManager;
        private final LedgerStorage storage;
        private final DiskChecker diskChecker;
        private final LedgerDirsManager ledgerDirsManager;
        private final LedgerDirsManager indexDirsManager;

        Resources(ServerConfiguration conf,
                  MetadataBookieDriver metadataDriver,
                  RegistrationManager registrationManager,
                  LedgerManagerFactory ledgerManagerFactory,
                  LedgerManager ledgerManager,
                  LedgerStorage storage,
                  DiskChecker diskChecker,
                  LedgerDirsManager ledgerDirsManager,
                  LedgerDirsManager indexDirsManager) {
            this.conf = conf;
            this.metadataDriver = metadataDriver;
            this.registrationManager = registrationManager;
            this.ledgerManagerFactory = ledgerManagerFactory;
            this.ledgerManager = ledgerManager;
            this.storage = storage;
            this.diskChecker = diskChecker;
            this.ledgerDirsManager = ledgerDirsManager;
            this.indexDirsManager = indexDirsManager;
        }

        void cleanup() {
            try {
                ledgerManager.close();
            } catch (Exception e) {
                log.warn("Error shutting down ledger manager", e);
            }
            try {
                ledgerManagerFactory.close();
            } catch (Exception e) {
                log.warn("Error shutting down ledger manager factory", e);
            }
            registrationManager.close();
            try {
                metadataDriver.close();
            } catch (Exception e) {
                log.warn("Error shutting down metadata driver", e);
            }
        }
    }

    /**
     * Builder for resources.
     */
    public static class ResourceBuilder {
        private final ServerConfiguration conf;
        private MetadataBookieDriver metadataBookieDriver;
        private RegistrationManager registrationManager;

        public ResourceBuilder(ServerConfiguration conf) {
            this.conf = conf;
        }

        Resources build() throws Exception {
            if (metadataBookieDriver == null) {
                if (conf.getMetadataServiceUri() == null) {
                    metadataBookieDriver = new NullMetadataBookieDriver();
                } else {
                    metadataBookieDriver = BookieResources.createMetadataDriver(conf, NullStatsLogger.INSTANCE);
                }
            }
            if (registrationManager == null) {
                registrationManager = metadataBookieDriver.createRegistrationManager();
            }
            LedgerManagerFactory ledgerManagerFactory = metadataBookieDriver.getLedgerManagerFactory();
            LedgerManager ledgerManager = ledgerManagerFactory.newLedgerManager();

            DiskChecker diskChecker = BookieResources.createDiskChecker(conf);
            LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                    conf, diskChecker, NullStatsLogger.INSTANCE);
            LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                    conf, diskChecker, NullStatsLogger.INSTANCE, ledgerDirsManager);

            LedgerStorage storage = BookieResources.createLedgerStorage(
                    conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

            return new Resources(conf,
                    metadataBookieDriver,
                    registrationManager,
                    ledgerManagerFactory,
                    ledgerManager,
                    storage,
                    diskChecker,
                    ledgerDirsManager,
                    indexDirsManager);
        }
    }
}