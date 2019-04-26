package de.hhu.bsinfo.dxram.loader.messages;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

public class SyncInvitationMessage extends Message {
    public SyncInvitationMessage() {
        super();
    }

    public SyncInvitationMessage(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_INVITATION);
    }
}
