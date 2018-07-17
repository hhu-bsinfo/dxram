package de.hhu.bsinfo.dxram.event;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the EventComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class EventComponentConfig extends AbstractDXRAMComponentConfig {
    private static final int THREAD_COUNT_MAX = 10;

    @Expose
    private int m_threadCount = 1;

    /**
     * Constructor
     */
    public EventComponentConfig() {
        super(EventComponent.class, true, true);
    }

    /**
     * Thread count for executor thread pool
     */
    public int getThreadCount() {
        return m_threadCount;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_threadCount < 1) {
            LOGGER.error("Invalid value (%d) for m_threadCount", m_threadCount);
            return false;
        }

        if (m_threadCount > THREAD_COUNT_MAX) {
            LOGGER.error("Max limit m_threadCount: %d", THREAD_COUNT_MAX);
            return false;
        }

        return true;
    }
}
