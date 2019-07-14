package de.hhu.bsinfo.dxram.loader.messages;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

public class FlushTableMessage extends Message {
    public FlushTableMessage() {
        super();
    }

    public FlushTableMessage(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_FLUSH_TABLE);
    }
}
