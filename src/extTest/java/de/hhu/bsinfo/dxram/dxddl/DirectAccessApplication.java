package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;

public abstract class DirectAccessApplication extends Application {


    @Override
    public void run() {
        final BootService bootService = getService(BootService.class);
        final ChunkService chunkService = getService(ChunkService.class);
        final ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
        DirectAccessSecurityManager.init(
            bootService,
            chunkLocalService.createLocal(),
            chunkLocalService.createReservedLocal(),
            chunkLocalService.reserveLocal(),
            chunkService.remove(),
            chunkLocalService.pinningLocal(),
            chunkLocalService.rawReadLocal(),
            chunkLocalService.rawWriteLocal());
        super.run();
    }
}
