
package de.hhu.bsinfo.utils.main;

import de.hhu.bsinfo.utils.ManifestHelper;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentListParser;
import de.hhu.bsinfo.utils.args.DefaultArgumentListParser;

/**
 * Framework for application execution with easier to handle argument list.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public abstract class AbstractMain {
	private String m_description = "";
	private ArgumentList m_arguments = new ArgumentList();
	private ArgumentListParser m_argumentsParser = new DefaultArgumentListParser();

	/**
	 * Constructor
	 * @param p_description
	 *            Short description for the application
	 */
	protected AbstractMain(final String p_description) {
		m_description = p_description;
	}

	/**
	 * Constructor
	 * @param p_argumentsParser
	 *            Provide a different parser for the program arguments.
	 * @param p_description
	 *            Short description for the application
	 */
	public AbstractMain(final ArgumentListParser p_argumentsParser, final String p_description) {
		m_argumentsParser = p_argumentsParser;
		m_description = p_description;
	}

	/**
	 * Print build date and user (if available).
	 * Requires the application to be built as a jar package.
	 */
	public void printBuildDateAndUser() {
		String buildDate = getBuildDate();
		if (buildDate != null) {
			System.out.println("BuildDate: " + buildDate);
		}
		String buildUser = getBuildUser();
		if (buildUser != null) {
			System.out.println("BuildUser: " + buildUser);
		}
	}

	/**
	 * Get the build date of the application. Has to be built as a jar file.
	 * @return If build date is available returns it as a string, null otherwise.
	 */
	public String getBuildDate() {
		return ManifestHelper.getProperty(getClass(), "BuildDate");
	}

	/**
	 * Get the user who built this application. Has to be built as a jar file.
	 * @return Name of the user who built this application if available, null otherwise.
	 */
	public String getBuildUser() {
		return ManifestHelper.getProperty(getClass(), "BuildUser");
	}

	/**
	 * Execute this application.
	 * @param p_args
	 *            Arguments from Java main entry point.
	 */
	public void run(final String[] p_args) {
		registerDefaultProgramArguments(m_arguments);
		m_argumentsParser.parseArguments(p_args, m_arguments);

		if (!m_arguments.checkArguments()) {
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
	 * @param p_arguments
	 *            Argument list for the application.
	 */
	protected abstract void registerDefaultProgramArguments(final ArgumentList p_arguments);

	/**
	 * Implement this and treat it as your main function.
	 * @param p_arguments
	 *            Arguments for the application.
	 * @return Application exit code.
	 */
	protected abstract int main(final ArgumentList p_arguments);
}
