package de.uniduesseldorf.dxram.core.migrate.messages;

public class MigrationMessages {
	// TODO adjust type as this is the same for chunk messages though we got different subtypes
	public static final byte TYPE = 10;
	public static final byte SUBTYPE_MIGRATION_REQUEST = 10;
	public static final byte SUBTYPE_MIGRATION_RESPONSE = 11;
	public static final byte SUBTYPE_MIGRATION_MESSAGE = 12;
}
