package de.hhu.bsinfo.ethnet.core;

/**
 * Created by nothaas on 6/13/17.
 */
public interface ConnectionManagerListener {

    void connectionCreated(final short p_destination);

    void connectionLost(final short p_destination);
}
