package de.hhu.bsinfo.dxram.failure.messages;

/**
 * Type and list of subtypes for all failure messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.10.2016
 */
public final class FailureMessages {
    public static final byte SUBTYPE_FAILURE_REQUEST = 1;
    public static final byte SUBTYPE_FAILURE_RESPONSE = 2;

    /**
     * Hidden constructor
     */
    private FailureMessages() {
    }
}
