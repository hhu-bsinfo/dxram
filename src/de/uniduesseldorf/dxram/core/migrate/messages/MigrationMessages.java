package de.uniduesseldorf.dxram.core.migrate.messages;

public class MigrationMessages {
	// TODO adjust type as this is the same for chunk messages though we got different subtypes
	public static final byte TYPE = 10;
	public static final byte SUBTYPE_MIGRATE_REQUEST = 10;
	public static final byte SUBTYPE_MIGRATE_RESPONSE = 11;
}
