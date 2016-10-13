package de.hhu.bsinfo.dxram.term;

/**
 * Created by nothaas on 10/13/16.
 */
public interface AbstractScriptCommand {

	/**
	 * Get name of command.
	 *
	 * @return String with name.
	 */
	String getName();

	/**
	 * Get a description of the command.
	 *
	 * @return Description message.
	 */
	String getDescription();
}
