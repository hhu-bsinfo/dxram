package de.hhu.bsinfo.dxram.event;

/**
 * Base class for events that can be fired within DXRAM.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public abstract class AbstractEvent {
    private String m_sourceClass;

    /**
     * Constructor
     *
     * @param p_sourceClass
     *     Source class this event originates from.
     */
    protected AbstractEvent(final String p_sourceClass) {
        m_sourceClass = p_sourceClass;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '[' + m_sourceClass + ']';
    }
}
