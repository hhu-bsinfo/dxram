package de.hhu.bsinfo.dxnet;

/**
 * Interface to listen to events from the ConnectionManager
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
public interface ConnectionManagerListener {

    /**
     * Called when a new connection was created
     *
     * @param p_destination
     *         Destination node id of the connection created
     */
    void connectionCreated(final short p_destination);

    /**
     * Called when a connection was lost
     *
     * @param p_destination
     *         Destination node id of the connection lost
     */
    void connectionLost(final short p_destination);
}
