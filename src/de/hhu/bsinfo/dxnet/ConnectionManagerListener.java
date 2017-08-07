package de.hhu.bsinfo.dxnet;

/**
 * Created by nothaas on 6/13/17.
 */
public interface ConnectionManagerListener {

    void connectionCreated(final short p_destination);

    void connectionLost(final short p_destination);
}
