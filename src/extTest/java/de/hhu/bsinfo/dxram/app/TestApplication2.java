package de.hhu.bsinfo.dxram.app;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class TestApplication2 extends AbstractApplication {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TestApplication2.class);

    private volatile boolean m_running = true;

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
        LOGGER.info("TEST");

        while (m_running) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                // ignore
            }
        }

        LOGGER.info("TEST DONE");
    }

    @Override
    public void signalShutdown() {
        m_running = false;
    }
}
