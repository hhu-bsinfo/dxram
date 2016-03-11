package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.args.ArgumentList;

public abstract class TerminalCommand 
{
	private TerminalDelegate m_terminalDelegate = null;
		
	/**
	 * Get name of command.
	 * @return the name
	 */
	public abstract String getName();

	/**
	 * Get a description of the command.
	 * @return Description message.
	 */
	public abstract String getDescription();

	public abstract void registerArguments(final ArgumentList p_arguments);

	public abstract boolean execute(final ArgumentList p_arguments);
	
	// --------------------------------------------------------------------------------------------

	void setTerminalDelegate(final TerminalDelegate p_terminalDelegate)
	{
		m_terminalDelegate = p_terminalDelegate;
	}

	protected TerminalDelegate getTerminalDelegate()
	{
		return m_terminalDelegate;
	}
}
