package de.hhu.bsinfo.utils.eval;

import java.util.HashMap;
import java.util.Map;

public class SimpleTable 
{
	private String m_intersectTopCornerName = new String("");
	private String[][] m_tableValues;
	private String[] m_rowNames;
	private Map<String, Integer> m_rowNameMapping = new HashMap<String, Integer>();
	private String[] m_columnNames;
	private Map<String, Integer> m_columnNameMapping = new HashMap<String, Integer>();
	
	public SimpleTable(final int p_rows, final int p_columns)
	{
		m_tableValues = new String[p_rows][p_columns];		
		for (int i = 0; i < m_tableValues.length; i++)
		{
			for (int j = 0; j < m_tableValues[i].length; j++)
			{
				m_tableValues[i][j] = new String();
			}
		}
		
		m_rowNames = new String[p_rows];
		for (int i = 0; i < m_rowNames.length; i++) {
			m_rowNames[i] = new String("Row " + i);
			m_rowNameMapping.put(m_rowNames[i], i);
		}
		
		m_columnNames = new String[p_columns];
		for (int i = 0; i < m_columnNames.length; i++) {
			m_columnNames[i] = new String("Column " + i);
			m_columnNameMapping.put(m_columnNames[i], i);
		}
	}
	
	public void setIntersectTopCornerName(final String p_name)
	{
		m_intersectTopCornerName = new String(p_name);
	}
	
	public void setRowName(final int p_row, final String p_name)
	{
		m_rowNames[p_row] = new String(p_name);
		m_rowNameMapping.put(p_name, p_row);
	}
	
	public void setColumnName(final int p_col, final String p_name)
	{
		m_columnNames[p_col] = new String(p_name);
		m_columnNameMapping.put(p_name, p_col);
	}
	
	public void set(final String p_row, final String p_column, final String p_value)
	{
		int rowIdx = m_rowNameMapping.get(p_row);
		int colIdx = m_columnNameMapping.get(p_column);
		m_tableValues[rowIdx][colIdx] = new String(p_value);
	}
	
	public void set(final int p_row, final int p_column, final String p_value)
	{
		m_tableValues[p_row][p_column] = new String(p_value);
	}
	
	public String toCsv()
	{
		return toCsv(true, ";");
	}
	
	public String toCsv(final boolean p_description, final String p_delimiter)
	{
		String str = new String();
		
		if (p_description) {
			str += m_intersectTopCornerName + p_delimiter;
		
			for (int i = 0; i < m_columnNames.length; i++) {
				str += m_columnNames[i] + p_delimiter;
			}
			
			str += "\n";
		}
		
		for (int i = 0; i < m_tableValues.length; i++)
		{
			if (p_description) {
				str += m_rowNames[i] + p_delimiter;
			}
			for (int j = 0; j < m_tableValues[i].length; j++)
			{
				str += m_tableValues[i][j] + p_delimiter;
			}
			str += "\n";
		}
		
		return str;
	}
	
	public String toString()
	{
		return toCsv(false, ",");
	}
}
