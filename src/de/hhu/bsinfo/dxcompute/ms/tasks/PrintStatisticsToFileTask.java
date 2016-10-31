
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

/**
 * Print the statistics to a file.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class PrintStatisticsToFileTask extends PrintStatisticsTask {

    private static final Logger LOGGER = LogManager.getFormatterLogger(PrintStatisticsToFileTask.class.getSimpleName());

    @Expose
    private String m_path;

    /**
     * Constructor
     * @param p_path
     *            Filepath of the file to print to.
     */
    public PrintStatisticsToFileTask(final String p_path) {
        super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_STATISTICS_FILE_TASK);
        m_path = p_path;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        BootService bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        StatisticsService statisticsService = p_ctx.getDXRAMServiceAccessor().getService(StatisticsService.class);

        if (m_path == null) {
            return -1;
        }

        File file = new File(m_path);
        if (file.exists()) {
            if (!file.delete()) {
                // #if LOGGER >= ERROR
                LOGGER.error("Deleting file %s failed", file);
                // #endif /* LOGGER >= ERROR */
                return -2;
            }
            try {
                if (!file.createNewFile()) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Creating output file %s for statistics failed", m_path);
                    // #endif /* LOGGER >= ERROR */
                    return -3;
                }
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Creating output file %s for statistics failed: %s", m_path, e);
                // #endif /* LOGGER >= ERROR */
                return -4;
            }
        }

        PrintStream out;
        try {
            out = new PrintStream(file);
        } catch (final FileNotFoundException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Creating print stream for statistics failed", e);
            // #endif /* LOGGER >= ERROR */
            return -5;
        }
        printStatisticsToOutput(out, bootService, statisticsService);

        out.close();

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }
}
