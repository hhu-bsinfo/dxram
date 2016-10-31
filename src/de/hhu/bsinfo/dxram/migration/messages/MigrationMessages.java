package de.hhu.bsinfo.dxram.migration.messages;

/**
 * Different migration message types.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public final class MigrationMessages {
    public static final byte SUBTYPE_MIGRATION_REQUEST = 1;
    public static final byte SUBTYPE_MIGRATION_RESPONSE = 2;
    public static final byte SUBTYPE_MIGRATION_REMOTE_MESSAGE = 3;

    /**
     * Hidden constructor
     */
    private MigrationMessages() {
    }
}
