package de.hhu.bsinfo.dxram.engine;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxram.app.ApplicationComponent;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkAnonService;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkDebugService;
import de.hhu.bsinfo.dxram.chunk.ChunkIndexComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkMigrationComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.failure.FailureComponent;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.job.JobWorkStealingComponent;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxram.monitoring.MonitoringService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.recovery.RecoveryService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;

public class DXRAMContextCreatorFileTest {
    private static final String CONF_PATH = "/tmp/dxram_test.conf";

    @Test
    public void test() {
        DXRAMComponentManager comps = new DXRAMComponentManager();

        comps.register(ApplicationComponent.class);
        comps.register(BackupComponent.class);
        comps.register(ChunkBackupComponent.class);
        comps.register(ChunkComponent.class);
        comps.register(ChunkIndexComponent.class);
        comps.register(ChunkMigrationComponent.class);
        comps.register(EventComponent.class);
        comps.register(FailureComponent.class);
        comps.register(JobWorkStealingComponent.class);
        comps.register(LogComponent.class);
        comps.register(LookupComponent.class);
        comps.register(NameserviceComponent.class);
        comps.register(NetworkComponent.class);
        comps.register(NullComponent.class);
        comps.register(ZookeeperBootComponent.class);

        DXRAMServiceManager services = new DXRAMServiceManager();

        services.register(ApplicationService.class);
        services.register(BootService.class);
        services.register(ChunkAnonService.class);
        services.register(ChunkDebugService.class);
        services.register(ChunkLocalService.class);
        services.register(ChunkService.class);
        services.register(JobService.class);
        services.register(LogService.class);
        services.register(LoggerService.class);
        services.register(LookupService.class);
        services.register(MasterSlaveComputeService.class);
        services.register(MigrationService.class);
        services.register(MonitoringService.class);
        services.register(NameserviceService.class);
        services.register(NetworkService.class);
        services.register(NullService.class);
        services.register(RecoveryService.class);
        services.register(StatisticsService.class);
        services.register(SynchronizationService.class);
        services.register(TemporaryStorageService.class);

        Assert.assertTrue(DXRAMContextCreatorFile.createDefaultConfiguration(CONF_PATH, comps, services));
        Assert.assertNotNull(DXRAMContextCreatorFile.loadConfiguration(CONF_PATH));
        new File(CONF_PATH).delete();
    }
}
