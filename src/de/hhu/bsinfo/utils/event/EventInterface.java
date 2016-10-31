package de.hhu.bsinfo.utils.event;

import de.hhu.bsinfo.dxram.event.AbstractEvent;

/**
 * Generic interface for any type of event handler.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.10.2016
 */
public interface EventInterface {
    /**
     * Fire an event.
     *
     * @param <T>
     *         Type of event to fire.
     * @param p_event
     *         Event to fire.
     */
    <T extends AbstractEvent> void fireEvent(T p_event);
}
