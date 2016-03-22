package de.hhu.bsinfo.utils.eval;

import java.util.HashMap;
import java.util.Map;

public class SimpleTables 
{
	private SimpleTable[] m_tables;
	private String[] m_tableNames;
	private Map<String, Integer> m_tableNameMappings = new HashMap<String, Integer>(); 
	
	public SimpleTables(final int p_tables, final int p_rows, final int p_columns)
	{
		m_tables = new SimpleTable[p_tables];
		for (int i = 0; i < m_tables.length; i++) {
			m_tables[i] = new SimpleTable(p_rows, p_columns);
		}
		
		m_tableNames = new String[p_tables];
		for (int i = 0; i < m_tableNames.length; i++) {
			m_tableNames[i] = new String("Table " + i);
			m_tableNameMappings.put(m_tableNames[i], i);
		}
	}
	
	public void setTableName(final int p_table, final String p_name)
	{
		m_tableNames[p_table] = p_name;
		m_tableNameMappings.put(p_name, p_table);
	}
	
	/// sets identical names for all intersection top corners in every table
	public void setIntersectTopCornerNames(final String p_name)
	{
		for (int i = 0; i < m_tables.length; i++) {
			m_tables[i].setIntersectTopCornerName(p_name);
		}
	}
	
	// sets identical names for a row in every table
	public void setRowNames(final int p_row, final String p_name)
	{
		for (int i = 0; i < m_tables.length; i++) {
			m_tables[i].setRowName(p_row, p_name);
		}
	}
	
	// sets identical names for a column in every table
	public void setColumnNames(final int p_col, final String p_name)
	{
		for (int i = 0; i < m_tables.length; i++) {
			m_tables[i].setColumnName(p_col, p_name);
		}
	}
	
	public void set(final String p_table, final String p_row, final String p_column, final String p_value)
	{
		int tableIdx = m_tableNameMappings.get(p_table);
		m_tables[tableIdx].set(p_row, p_column, p_value);
	}
	
	public void set(final int p_table, final int p_row, final int p_column, final String p_value)
	{
		m_tables[p_table].set(p_row, p_column, p_value);
	}
	
	public String toCsv()
	{
		return toCsv(true, ";");
	}
	
	public String toCsv(final boolean p_description, final String p_delimiter)
	{
		String str = new String();
		
		for (int i = 0; i < m_tables.length; i++) {
			if (p_description) {
				str += m_tableNames[i] + "\n";
			}
			str += m_tables[i].toCsv(p_description, p_delimiter);
			str += "\n";
		}
		
		return str;
	}
	
	public String toString()
	{
		return toCsv(false, ",");
	}
}
