package de.hhu.bsinfo.dxram.app;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class TestApplicationAccessor extends AbstractApplication {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TestApplicationAccessor.class);

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        // always build this test against the latest version
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "test";
    }

    @Override
    public void main(final String[] p_args) {
        BootService boot = getService(BootService.class);

        LOGGER.info("My node ID: %X", boot.getNodeID());
    }

    @Override
    public void signalShutdown() {

    }
}
