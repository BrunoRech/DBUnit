package junitdbunit;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.dbunit.DBTestCase;
import org.dbunit.PropertiesBasedJdbcDatabaseTester;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Test;

public class DbUnit extends DBTestCase {

	public DbUnit(String name) {
		super(name);
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_DRIVER_CLASS, "org.postgresql.Driver");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_CONNECTION_URL, "jdbc:postgresql://localhost:5432/dbunit");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_USERNAME, "bruno");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_PASSWORD, "postgre");
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
	 * Testa a quantidade de usu√°rios na tabela de usu√°rios
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
	 * Compara o nome do usu√°rio na primeira linha da tabela de usu√°rios
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
	 * Verifica se o √∫ltimo usu√°rio da lista de usu√°rios possue a habilita√ß√£o
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

        //Testa se a atualizaÁ„o do valor est· sendo feita 
        @Test
        public void testSQLUpdate() throws Exception{
                Connection con = getSimpleConnection();
                Statement stmt = con.createStatement();
                // Pega o valor atual
                ResultSet rst = stmt.executeQuery("SELECT * FROM usuarios WHERE id = 1");
                if(rst.next()){
                    System.out.println("nome_completo " + rst.getString("nome_completo"));
                        // compara a partir de dataset.xml
                        assertEquals("Peter Parker", rst.getString("nome_completo"));
                        rst.close();

                        // atualiza via SQL
                        int count = stmt.executeUpdate("UPDATE usuarios SET nome_completo='Porco Aranha' WHERE id=1");

                        //stmt.close();
                        //con.close();

                        // expera somente 1 linha de alteraÁ„o
                        assertEquals("one row should be updated", 1, count);
                        System.out.println("count deve ser 1---" + count);
                        

                        ResultSet after = stmt.executeQuery("SELECT * FROM usuarios WHERE id = 1");
                        if(after.next()){
                            System.out.println("nome_completo " + after.getString("nome_completo"));
                            // compara a partir de dataset.xml
                            assertEquals("Porco Aranha", after.getString("nome_completo"));
                            after.close();

                        }
                        
                        // Fetch database data after executing the code
                        QueryDataSet databaseSet = new QueryDataSet(getConnection());
                        // filtra os dados
                        databaseSet.addTable("usuario", "select * from pessoas where id = 1");
                        ITable actualTable = databaseSet.getTables()[0];

                        // Carrega os dados esperados a partir do XML 
                        IDataSet expectedDataSet = new FlatXmlDataSetBuilder().build(new FileInputStream("expectedDataSet.xml"));
                        ITable expectedTable = expectedDataSet.getTable("usuario");

                        // Filtra colunas desnessarias dos dados atuais definidos pelo XML
                        actualTable = DefaultColumnFilter.includedColumnsTable(actualTable, expectedTable.getTableMetaData().getColumns());

                        // Assert da base de dados atual com os dados esperados
                        assertEquals(1,expectedTable.getRowCount());
                        assertEquals(expectedTable.getRowCount(), actualTable.getRowCount());
                        assertEquals(expectedTable.getValue(0, "nome"), actualTable.getValue(0, "nome"));
            
                } else {
                        fail("no rows");
                        rst.close();
                        stmt.close();
                        con.close();
                }
    }

    private Connection getSimpleConnection(){
        Connection con = null;
            try {
                con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/dbunit", "bruno","postgre");
            } catch (SQLException ex) {
                Logger.getLogger(DbUnit.class.getName()).log(Level.SEVERE, null, ex);
            }
        return con;
    } 
        
}
