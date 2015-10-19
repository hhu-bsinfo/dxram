
package de.uniduesseldorf.dxram.core.recovery;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.exceptions.RecoveryException;

/**
 * Methods for accessing the Recovery-Service
 * @author Florian Klein
 *         06.08.2012
 */
public interface RecoveryInterface extends CoreComponent {

	// Methods
	/**
	 * Recovers all Chunks of given node
	 * @param p_nodeID
	 *            the NodeID of the node whose Chunks have to be restored
	 * @param p_dest
	 *            the NodeID of the node where the Chunks have to be restored
	 * @return whether the operation was successful or not
	 * @throws RecoveryException
	 *             if the recovery fails
	 * @throws LookupException
	 *             if the backup peers could not be determined
	 */
	boolean recover(short p_nodeID, short p_dest) throws RecoveryException, LookupException;

}
