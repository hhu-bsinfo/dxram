
package de.hhu.bsinfo.dxram.migration.messages;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a migration request.
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class MigrationResponse extends AbstractResponse {

	/**
	 * Creates an instance of DataResponse.
	 * This constructor is used when receiving this message.
	 */
	public MigrationResponse() {
		super();
	}

	/**
	 * Creates an instance of DataResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request
	 */
	public MigrationResponse(final MigrationRequest p_request) {
		super(p_request, MigrationMessages.SUBTYPE_MIGRATION_RESPONSE);
	}
}
