package de.hhu.bsinfo.menet;

/**
 * Represents an Action
 * @param <T>
 *            the type of the information object
 * @author Florian Klein
 *         13.04.2012
 */
public abstract class AbstractAction<T> {

	// Constructors
	/**
	 * Creates an instance of AbstractAction
	 */
	public AbstractAction() {}

	// Methods
	/**
	 * Executes the Action
	 */
	public final void execute() {
		execute(null);
	}

	/**
	 * Executes the Action
	 * @param p_object
	 *            an object with further information
	 */
	public abstract void execute(T p_object);

}
