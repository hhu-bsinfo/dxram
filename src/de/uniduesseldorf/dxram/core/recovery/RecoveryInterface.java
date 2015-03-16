
package de.uniduesseldorf.dxram.core.recovery;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.RecoveryException;

/**
 * Methods for accessing the Recovery-Service
 * @author Florian Klein
 *         06.08.2012
 */
public interface RecoveryInterface extends CoreComponent {

	// Methods
	/**
	 * Recovers a Chunk of the given ID from disc
	 * @param p_chunkID
	 *            the ID
	 * @return the recovered Chunk
	 * @throws RecoveryException
	 *             if the recovery fails
	 */
	Chunk recover(long p_chunkID) throws RecoveryException;

	/**
	 * Recovers all Chunks of the own node
	 * @return the recovered Chunks
	 * @throws RecoveryException
	 *             if the recovery fails
	 */
	Chunk[] recoverAll() throws RecoveryException;

}
