
package de.hhu.bsinfo.dxram.lock.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.ethnet.AbstractResponse;
import de.hhu.bsinfo.utils.Pair;

/**
 * Response to a LockedListRequest
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.04.2016
 */
public class GetLockedListResponse extends AbstractResponse {

    private ArrayList<Pair<Long, Short>> m_list;

    /**
     * Creates an instance of LockResponse as a receiver.
     */
    public GetLockedListResponse() {
        super();
    }

    /**
     * Creates an instance of LockResponse as a sender.
     * @param p_request
     *            Corresponding request to this response.
     * @param p_lockedList
     *            List of locked chunks to send
     */
    public GetLockedListResponse(final GetLockedListRequest p_request, final ArrayList<Pair<Long, Short>> p_lockedList) {
        super(p_request, LockMessages.SUBTYPE_GET_LOCKED_LIST_RESPONSE);

        m_list = p_lockedList;
    }

    /**
     * Get the list of locked chunks.
     * @return List of locked chunks.
     */
    public ArrayList<Pair<Long, Short>> getList() {
        return m_list;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_list.size());
        for (Pair<Long, Short> entry : m_list) {
            p_buffer.putLong(entry.first());
            p_buffer.putShort(entry.second());
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int size = p_buffer.getInt();
        m_list = new ArrayList<Pair<Long, Short>>(size);
        for (int i = 0; i < size; i++) {
            m_list.add(new Pair<Long, Short>(p_buffer.getLong(), p_buffer.getShort()));
        }
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_list.size() * (Long.BYTES + Short.BYTES);
    }
}
