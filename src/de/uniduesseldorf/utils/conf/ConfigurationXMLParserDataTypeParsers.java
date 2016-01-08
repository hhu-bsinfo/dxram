package de.uniduesseldorf.utils.conf;

import de.uniduesseldorf.utils.conf.ConfigurationXMLParser.DataTypeParser;

public class ConfigurationXMLParserDataTypeParsers 
{
	public static class String implements DataTypeParser
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "str";
		}

		@Override
		public Object parse(java.lang.String p_str) {
			return new java.lang.String(p_str);
		}
	}
	
	public static class Byte implements DataTypeParser
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "byte";
		}

		@Override
		public Object parse(java.lang.String p_str) {
			return java.lang.Byte.parseByte(p_str);
		}
	}
	
	public static class Short implements DataTypeParser
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "short";
		}

		@Override
		public Object parse(java.lang.String p_str) {
			return java.lang.Short.parseShort(p_str);
		}
	}
	
	public static class Int implements DataTypeParser
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "int";
		}

		@Override
		public Object parse(java.lang.String p_str) {
			return java.lang.Integer.parseInt(p_str);
		}
	}
	
	public static class Long implements DataTypeParser
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "long";
		}

		@Override
		public Object parse(java.lang.String p_str) {
			return java.lang.Long.parseLong(p_str);
		}
	}
	
	public static class Float implements DataTypeParser
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "float";
		}

		@Override
		public Object parse(java.lang.String p_str) {
			return java.lang.Float.parseFloat(p_str);
		}
	}
	
	public static class Double implements DataTypeParser
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "double";
		}

		@Override
		public Object parse(java.lang.String p_str) {
			return java.lang.Double.parseDouble(p_str);
		}
	}
	
	public static class Bool implements DataTypeParser
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "bool";
		}

		@Override
		public Object parse(java.lang.String p_str) {
			// allow 0 and 1 as well
			if (p_str.equals("0")) {
				return new java.lang.Boolean(false);
			} else if (p_str.equals("1")) {
				return new java.lang.Boolean(true);
			} else {
				return java.lang.Boolean.parseBoolean(p_str);
			}
		}
	}
	
	public static class Boolean extends Bool
	{
		@Override
		public java.lang.String getTypeIdentifer() {
			return "boolean";
		}
	}
}
