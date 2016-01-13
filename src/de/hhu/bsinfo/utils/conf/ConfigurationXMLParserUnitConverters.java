package de.hhu.bsinfo.utils.conf;

import de.hhu.bsinfo.utils.conf.ConfigurationXMLParser.UnitConverter;

public class ConfigurationXMLParserUnitConverters 
{
	public static class KiloByteToByte implements UnitConverter
	{
		@Override
		public String getUnitIdentifier() {
			return "kb2b";
		}

		@Override
		public Object convert(final Object p_value) {
			if (p_value instanceof Byte) {
				return ((Byte) p_value) * 1024;
			} else if (p_value instanceof Short) {
				return ((Short) p_value) * 1024;
			} else if (p_value instanceof Integer) {
				return ((Integer) p_value) * 1024;
			} else if (p_value instanceof Long) {
				return ((Long) p_value) * 1024;
			} else {
				return p_value;
			}
		}
	}
	
	public static class MegaByteToByte implements UnitConverter
	{
		@Override
		public String getUnitIdentifier() {
			return "mb2b";
		}

		@Override
		public Object convert(final Object p_value) {
			if (p_value instanceof Byte) {
				return ((Byte) p_value) * 1024 * 1024;
			} else if (p_value instanceof Short) {
				return ((Short) p_value) * 1024 * 1024;
			} else if (p_value instanceof Integer) {
				return ((Integer) p_value) * 1024 * 1024;
			} else if (p_value instanceof Long) {
				return ((Long) p_value) * 1024 * 1024;
			} else {
				return p_value;
			}
		}
	}
	
	public static class GigabyteByteToByte implements UnitConverter
	{
		@Override
		public String getUnitIdentifier() {
			return "gb2b";
		}

		@Override
		public Object convert(final Object p_value) {
			if (p_value instanceof Byte) {
				return ((Byte) p_value) * 1024 * 1024 * 1024;
			} else if (p_value instanceof Short) {
				return ((Short) p_value) * 1024 * 1024 * 1024;
			} else if (p_value instanceof Integer) {
				return ((Integer) p_value) * 1024 * 1024 * 1024;
			} else if (p_value instanceof Long) {
				return ((Long) p_value) * 1024 * 1024 * 1024;
			} else {
				return p_value;
			}
		}
	}
}
