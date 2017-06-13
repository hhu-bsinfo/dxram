package de.hhu.bsinfo.net;

/**
 * Created by nothaas on 6/13/17.
 */
public interface MessageReceiverStore {
    MessageReceiver getReceiver(final byte p_type, final byte p_subtype);
}
