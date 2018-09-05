package de.hhu.bsinfo.dxram.app;

/**
 * Callback handler to receive application state changes
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.09.18
 */
public interface ApplicationCallbackHandler {
    /**
     * Application has started
     *
     * @param p_application
     *         Application instance
     */
    void started(final AbstractApplication p_application);

    /**
     * Application has finished
     *
     * @param p_application
     *         Application instance
     */
    void finished(final AbstractApplication p_application);
}
