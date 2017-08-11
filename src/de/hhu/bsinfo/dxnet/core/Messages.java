package de.hhu.bsinfo.dxnet.core;

/**
 * Message types reserved for the network subsystem
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.07.2017
 */
public final class Messages {
    public static final byte NETWORK_MESSAGES_TYPE = 0;

    public static final byte SUBTYPE_INVALID_MESSAGE = 0;
    public static final byte SUBTYPE_DEFAULT_MESSAGE = 1;

    /**
     * Hidden constructor
     */
    private Messages() {
    }
}
