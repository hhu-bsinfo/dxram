
package de.hhu.bsinfo.dxram.engine;

/**
 * Dummy component implementation for testing.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NullComponent extends AbstractDXRAMComponent {

	/**
	 * Constructor
	 */
	public NullComponent() {
		super(1, 99);
	}

	@Override
	protected boolean initComponent(final DXRAMContext.EngineSettings p_engineSettings) {
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}
}
