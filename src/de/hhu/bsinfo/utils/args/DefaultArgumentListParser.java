
package de.hhu.bsinfo.utils.args;

/**
 * Default parser for arguments provided for an application.
 * Example: bla{kb2b}:1234594 bla2:5555
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class DefaultArgumentListParser implements ArgumentListParser {

	private static final String KEY_VAL_SEPARATOR = ":";
	private static final String UNIT_PREFIX = "{";
	private static final String UNIT_POSTFIX = "}";

	/**
	 * Constructor
	 */
	public DefaultArgumentListParser() {

	}

	@Override
	public void parseArguments(final String[] p_args, final ArgumentList p_arguments) {
		for (int i = 0; i < p_args.length; i++) {
			String[] keyVal = splitKeyValue(p_args[i]);
			// ignore invalid format
			if (keyVal == null) {
				continue;
			}

			// allow strings within " "
			if (keyVal[1].charAt(0) == '"') {
				// concat args within astring
				// don't count if quote is escpaed
				while (keyVal[1].charAt(keyVal[1].length() - 1) != '"') {
					if (i + 1 < p_args.length) {
						keyVal[1] += p_args[i + 1];
						i++;
					} else {
						break;
					}
				}

				// remove quotes
				if (keyVal[1].charAt(0) == '"') {
					keyVal[1] = keyVal[1].substring(1);
				}
				if (keyVal[1].charAt(keyVal[1].length() - 1) == '"') {
					keyVal[1] = keyVal[1].substring(0, keyVal[1].length() - 1);
				}
			} else {
				// concat args which have escpaed spaces
				while (keyVal[1].charAt(keyVal[1].length() - 1) == '\\') {
					if (i + 1 < p_args.length) {
						keyVal[1] = keyVal[1].substring(0, keyVal[1].length() - 1);
						keyVal[1] += p_args[i + 1];
						i++;
					} else {
						break;
					}
				}
			}

			String keyName = getKeyName(keyVal[0]);
			String unit = getKeyUnit(keyVal[0]);

			p_arguments.setArgument(keyName, keyVal[1], unit);
		}
	}

	/**
	 * Split key value tuple bla{kb2b}:1234594 -> bla{kb2b} and 1234594
	 * @param p_argument
	 *            String to split.
	 * @return Key value tuple
	 */
	private String[] splitKeyValue(final String p_argument) {
		// don't use split here. the value can contain the separator as well
		int sepIndex = p_argument.indexOf(KEY_VAL_SEPARATOR);
		if (sepIndex == -1) {
			return null;
		}

		String[] keyVal = new String[2];
		keyVal[0] = p_argument.substring(0, sepIndex);
		keyVal[1] = p_argument.substring(sepIndex + 1);
		return keyVal;
	}

	/**
	 * Get the name from the key: bla{kb2b} -> bla
	 * @param p_key
	 *            Key provided.
	 * @return Name part of the key.
	 */
	private String getKeyName(final String p_key) {
		int typeStart = p_key.indexOf(UNIT_PREFIX);

		// no type attached, default to string
		if (typeStart == -1) {
			return p_key;
		} else {
			return p_key.substring(0, typeStart);
		}
	}

	/**
	 * Get the unit part of the key: bla{kb2b}:1234594 -> kb2b
	 * @param p_key
	 *            Key provided.
	 * @return Unit part of the key or empty string if not available.
	 */
	private String getKeyUnit(final String p_key) {
		int typeStart = p_key.indexOf(UNIT_PREFIX);
		int typeEnd = p_key.indexOf(UNIT_POSTFIX);

		// no type attached, default to string
		if (typeStart == -1 || typeEnd == -1) {
			return "";
		} else {
			return p_key.substring(typeStart + 1, typeEnd);
		}
	}
}
