
package de.hhu.bsinfo.dxram.lock.messages;

/**
 * Network message types for the lock package
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.01.2016
 */
public final class LockMessages {
    public static final byte SUBTYPE_LOCK_REQUEST = 1;
    public static final byte SUBTYPE_LOCK_RESPONSE = 2;
    public static final byte SUBTYPE_UNLOCK_MESSAGE = 3;
    public static final byte SUBTYPE_GET_LOCKED_LIST_REQUEST = 4;
    public static final byte SUBTYPE_GET_LOCKED_LIST_RESPONSE = 5;

    /**
     * Static class
     */
    private LockMessages() {
    }
}
