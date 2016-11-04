package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a ReplaceBackupPeerRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 21.10.2016
 */
public class ReplaceBackupPeerResponse extends AbstractResponse {

    // Constructors

    /**
     * Creates an instance of ReplaceBackupPeerResponse
     */
    public ReplaceBackupPeerResponse() {
        super();
    }

    /**
     * Creates an instance of ReplaceBackupPeerResponse
     *
     * @param p_request
     *     the corresponding ReplaceBackupPeerRequest
     */
    public ReplaceBackupPeerResponse(final ReplaceBackupPeerRequest p_request) {
        super(p_request, LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_RESPONSE);
    }

}
