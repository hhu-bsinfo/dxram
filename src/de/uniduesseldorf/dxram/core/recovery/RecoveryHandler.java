package de.uniduesseldorf.dxram.core.recovery;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.RecoveryException;

/**
 * Implements the Recovery-Service
 * @author Florian Klein 06.08.2012
 */
public final class RecoveryHandler implements RecoveryInterface {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(RecoveryHandler.class);

	// Constructors
	/**
	 * Creates an instance of RecoveryHandler
	 */
	public RecoveryHandler() {}

	// Methods
	@Override
	public void initialize() throws DXRAMException {
		LOGGER.trace("Entering initialize");

		LOGGER.trace("Exiting initialize");
	}

	@Override
	public void close() {
		LOGGER.trace("Entering close");

		LOGGER.trace("Exiting close");
	}

	@Override
	public Chunk recover(final long p_chunkID) throws RecoveryException {
		return null;
	}

	@Override
	public Chunk[] recoverAll() throws RecoveryException {
		return null;
	}

}
