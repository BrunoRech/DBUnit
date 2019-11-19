package junitdbunit;

import java.io.FileInputStream;

import org.dbunit.DBTestCase;
import org.dbunit.PropertiesBasedJdbcDatabaseTester;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Test;

public class DbUnit extends DBTestCase {

	public DbUnit(String name) {
		super(name);
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_DRIVER_CLASS, "org.postgresql.Driver");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_CONNECTION_URL, "jdbc:postgresql://localhost:5432/dbunit");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_USERNAME, "bruno");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_PASSWORD, "postgres");
	}

	protected IDataSet getDataSet() throws Exception {
		return new FlatXmlDataSetBuilder().build(new FileInputStream("user.xml"));
	}

	protected DatabaseOperation getSetUpOperation() throws Exception {
		return DatabaseOperation.REFRESH;
	}

	protected DatabaseOperation getTearDownOperation() throws Exception {
		return DatabaseOperation.NONE;
	}

	/**
	 * Testa a quantidade de usuários na tabela de usuários
	 */
	@Test
	public void testUserRowCount() {
		
		try {
			IDataSet databaseDataSet = getConnection().createDataSet();
			int actual = databaseDataSet.getTable("usuarios").getRowCount();
			assertEquals(6, actual);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Compara o nome do usuário na primeira linha da tabela de usuários
	*/
	@Test
	public void testFirstRowName() {
		try {
			IDataSet databaseDataSet = getConnection().createDataSet();
			String actual = (String) databaseDataSet.getTable("usuarios").getValue(0, "nome_completo");
			assertEquals("Peter Parker", actual);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Verifica se o último usuário da lista de usuários possue a habilitação
	 */
	 @Test 
	 public void testLastUserIsAble() {
		 try {
				IDataSet databaseDataSet = getConnection().createDataSet();
				int rows = databaseDataSet.getTable("usuarios").getRowCount();
				boolean actual = (boolean) databaseDataSet.getTable("usuarios").getValue(rows - 1, "habilitado");
				assertEquals(true, actual);
			} catch (Exception e) {
				e.printStackTrace();
			}
	 }

}
