
package de.hhu.bsinfo.dxram.boot.messages;

/**
 * Type and list of subtypes for all boot messages
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.05.2016
 */
public final class BootMessages {
    public static final byte SUBTYPE_REBOOT_MESSAGE = 1;
    public static final byte SUBTYPE_SHUTDOWN_MESSAGE = 2;

    /**
     * Static class
     */
    private BootMessages() {
    }
}
