
package de.hhu.bsinfo.dxram.recovery;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.exceptions.RecoveryException;

import de.hhu.bsinfo.dxram.lookup.LookupException;

/**
 * Methods for accessing the Recovery-Service
 * @author Florian Klein
 *         06.08.2012
 */
public interface RecoveryInterface extends CoreComponent {

	// Methods
	/**
	 * Recovers all Chunks of given node
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 * @param p_dest
	 *            the NodeID of the node where the Chunks have to be restored
	 * @param p_useLiveData
	 *            whether the recover should use current logs or log files
	 * @return whether the operation was successful or not
	 * @throws RecoveryException
	 *             if the recovery fails
	 * @throws LookupException
	 *             if the backup peers could not be determined
	 */
	boolean recover(short p_owner, short p_dest, boolean p_useLiveData) throws RecoveryException, LookupException;

}
