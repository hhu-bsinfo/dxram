package de.uniduesseldorf.dxram.core.lock.messages;

public class LockMessages {
	// TODO adjust type as this is the same for chunk messages though we got different subtypes
	public static final byte TYPE = 10;
	public static final byte SUBTYPE_LOCK_REQUEST = 7;
	public static final byte SUBTYPE_LOCK_RESPONSE = 8;
	public static final byte SUBTYPE_UNLOCK_MESSAGE = 9;
}
