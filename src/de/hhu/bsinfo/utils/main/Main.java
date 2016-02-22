package de.hhu.bsinfo.utils.main;

import de.hhu.bsinfo.utils.args.DefaultArgumentListParser;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentListParser;

/**
 * Framwork for application execution with easier to handle argument list.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public abstract class Main
{
	private String m_description = new String();
	private ArgumentList m_arguments = new ArgumentList();
	private ArgumentListParser m_argumentsParser = new DefaultArgumentListParser();
	
	/**
	 * Constructor
	 */
	protected Main(final String p_description) {
		m_description = p_description;
	}
	
	/**
	 * Constructor
	 * @param p_argumentsParser Provide a different parser for the program arguments.
	 */
	public Main(final ArgumentListParser p_argumentsParser, final String p_description) {
		m_argumentsParser = p_argumentsParser;
		m_description = p_description;
	}
	
	/**
	 * Execute this application.
	 * @param args Arguments from Java main entry point.
	 */
	public void run(final String[] args) {
		registerDefaultProgramArguments(m_arguments);
		m_argumentsParser.parseArguments(args, m_arguments);
		
		if (!m_arguments.checkArguments())
		{
			String usage = m_arguments.createUsageDescription(getClass().getName());
			System.out.println(getClass().getName() + ": " + m_description + "\n");
			System.out.println(usage);
			System.exit(-1);
		}
		
		int exitCode = main(m_arguments);
		
		System.exit(exitCode);
	}
	
	/**
	 * Implement this and provide default arguments the application expects.
	 * @param p_arguments Argument list for the application.
	 */
	protected abstract void registerDefaultProgramArguments(final ArgumentList p_arguments);
	
	/**
	 * Implement this and treat it as your main function.
	 * @param p_arguments Arguments for the application.
	 * @return Application exit code.
	 */
	protected abstract int main(final ArgumentList p_arguments);
}
