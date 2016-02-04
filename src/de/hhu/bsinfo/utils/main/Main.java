package de.hhu.bsinfo.utils.main;

/**
 * Framwork for application execution with easier to handle argument list.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public abstract class Main
{
	private MainArguments m_arguments = new MainArguments();
	private MainArgumentsParser m_argumentsParser = new DefaultMainArgumentsParser();
	
	/**
	 * Constructor
	 */
	protected Main() {
		
	}
	
	/**
	 * Constructor
	 * @param p_argumentsParser Provide a different parser for the program arguments.
	 */
	public Main(final MainArgumentsParser p_argumentsParser) {
		m_argumentsParser = p_argumentsParser;
	}
	
	/**
	 * Execute this application.
	 * @param args Arguments from Java main entry point.
	 */
	public void run(final String[] args) {
		registerDefaultProgramArguments(m_arguments);
		m_argumentsParser.parseArguments(args, m_arguments);
		
		int exitCode = main(m_arguments);
		
		System.exit(exitCode);
	}
	
	/**
	 * Implement this and provide default arguments the application expects.
	 * @param p_arguments Argument list for the application.
	 */
	protected abstract void registerDefaultProgramArguments(final MainArguments p_arguments);
	
	/**
	 * Implement this and treat it as your main function.
	 * @param p_arguments Arguments for the application.
	 * @return Application exit code.
	 */
	protected abstract int main(final MainArguments p_arguments);
}
