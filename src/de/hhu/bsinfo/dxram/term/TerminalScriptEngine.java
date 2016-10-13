package de.hhu.bsinfo.dxram.term;

interface TerminalScriptEngine {

	String getHelp();

	boolean setupDXRAMContext(final DXRAMContext p_dxramContext);

	boolean loadScriptFile(final String p_path);

	boolean loadScriptCommandFile(final String p_path);

	boolean evaluate(final String p_text);
}
