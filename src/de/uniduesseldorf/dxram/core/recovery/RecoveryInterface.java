
package de.uniduesseldorf.dxram.core.recovery;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
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
	 * @return the recovered Chunks
	 * @throws RecoveryException
	 *             if the recovery fails
	 * @throws LookupException
	 *             if the backup peers could not be determined
	 */
	Chunk[] recover(short p_nodeID) throws RecoveryException, LookupException;

}
