package de.hhu.bsinfo.dxram.stats;

import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;

/**
 * Service for internal statistics
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2017
 */
@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class StatisticsService extends Service<StatisticsServiceConfig> {
    private PrinterThread m_printerThread;

    /**
     * Get the statistics manager
     */
    public StatisticsManager getManager() {
        return StatisticsManager.get();
    }

    @Override
    protected void resolveComponentDependencies(final ComponentProvider p_componentAccessor) {

    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        int printThreadPeriodMs = getConfig().getPrintStatsPeriodMs();

        if (printThreadPeriodMs > 0) {
            LOGGER.info("Statistics printer thread enabled (%d ms)", printThreadPeriodMs);

            m_printerThread = new PrinterThread(printThreadPeriodMs);
            m_printerThread.start();
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
        if (m_printerThread != null) {
            m_printerThread.shutdown();

            try {
                m_printerThread.join();
            } catch (InterruptedException ignored) {
            }
        }

        return true;
    }

    private static class PrinterThread extends Thread {
        private final int m_printIntervalMs;

        private volatile boolean m_running;

        public PrinterThread(final int p_printIntervalMs) {
            m_printIntervalMs = p_printIntervalMs;
            m_running = true;
        }

        public void shutdown() {
            m_running = false;
            interrupt();
        }

        @Override
        public void run() {
            while (m_running) {
                StatisticsManager.get().printStatistics(System.out);

                try {
                    Thread.sleep(m_printIntervalMs);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
