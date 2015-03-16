
package de.uniduesseldorf.dxram.core;

import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Methods for an DXRAM-Component
 * @author Florian Klein
 *         14.03.2012
 */
public interface CoreComponent {

	// Methods
	/**
	 * Initializes component<br>
	 * Should be called before any other method call of the component
	 * @throws DXRAMException
	 *             if the component could not be initialized
	 */
	void initialize() throws DXRAMException;

	/**
	 * Closes Component und frees unused ressources
	 */
	void close();

}
