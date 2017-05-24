package de.hhu.bsinfo.dxram.event;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the EventComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class EventComponentConfig extends DXRAMComponentConfig {
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
}
