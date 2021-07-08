package tests;

import org.testng.annotations.Test;
import structure.TestBasis;

import java.sql.SQLException;
import java.util.Arrays;

import static org.testng.Assert.*;

//Запустить windows service

public class TestExecutor extends TestBasis {
    @Test(priority = 1)
    public void createDatabase() {
        generalPage.setupConnection("", user, password);
        generalPage.setupAndExecuteStatement("CREATE DATABASE VEHICLES");
        System.out.println("Schema created");
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "createDatabase")
    public void createTable() {
        generalPage.setupConnection("VEHICLES", user, password);
        String sqlRequestCreateTable = "CREATE TABLE CARS " +
                "(id INTEGER not NULL, " +
                " speed VARCHAR(255), " +
                " colour VARCHAR(255), " +
                " year INTEGER, " +
                " PRIMARY KEY ( id ))";
        generalPage.setupAndExecuteStatement(sqlRequestCreateTable);
        System.out.println("Table created");
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "createTable")
    public void insertIntoTable() {
        generalPage.setupConnection("VEHICLES", user, password);
        String sqlInsertStatement = "INSERT INTO `vehicles`.`cars` (`id`, `speed`, `colour`, `year`) \n" +
                "VALUES ('1', '300', 'black', 1998), ('2', '320', 'yellow', 2000)";
        generalPage.setupAndExecuteStatement(sqlInsertStatement);
        System.out.println("Search performed");
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "insertIntoTable")
    public void assertAddedInformation() throws SQLException {
        generalPage.setupConnection("VEHICLES", user, password);
        String sqlSearchAllStatements = "select * from vehicles.cars";
        generalPage.setupResultSetAndExecuteStatement(sqlSearchAllStatements);
        assertTrue(Arrays.toString(generalPage.getInfoFromAllRows(4).get(1)).contains("yellow"),
                "Second added row is yellow car");
        System.out.println("Assertion passed - car is yellow");
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "assertAddedInformation")
    public void updateTable() {
        generalPage.setupConnection("VEHICLES", user, password);
        String sqlUpdateStatement = "UPDATE `vehicles`.`cars` " +
                "SET `colour` = 'orange' " +
                "WHERE (`id` = '2')";
        generalPage.setupAndExecuteStatement(sqlUpdateStatement);
        System.out.println("Row 2 is updated. Second car is now orange");
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "updateTable")
    public void assertAddedCarIsOrange() throws SQLException {
        generalPage.setupConnection("VEHICLES", user, password);
        String sqlSearchAllStatements = "select * from vehicles.cars";
        generalPage.setupResultSetAndExecuteStatement(sqlSearchAllStatements);
        assertTrue(Arrays.toString(generalPage.getInfoFromAllRows(4).get(1)).contains("orange"),
                "Second added car is orange");
        System.out.println("Assertion passed - car is orange");
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "assertAddedCarIsOrange")
    public void deleteFromTable() {
        generalPage.setupConnection("VEHICLES", user, password);
        String sqlDeleteStatement = "DELETE FROM `vehicles`.`cars` WHERE (`id` = '2')";
        generalPage.setupAndExecuteStatement(sqlDeleteStatement);
        System.out.println("Row 2 is deleted");
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "deleteFromTable")
    public void assertRowDeleted() throws SQLException {
        generalPage.setupConnection("VEHICLES", user, password);
        String sqlSearchAllStatements = "select * from vehicles.cars";
        generalPage.setupResultSetAndExecuteStatement(sqlSearchAllStatements);
        assertEquals(generalPage.getInfoFromAllRows(4).size(), 1, "Table contains only one row");
        System.out.println("Table contains only one row");
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "assertRowDeleted")
    public void dropTable() throws SQLException {
        generalPage.setupConnection("VEHICLES", user, password);
        String sqlDeleteTableStatement = "DROP TABLE `vehicles`.`cars`";
        generalPage.createStatement();
        generalPage.dropTable("cars", sqlDeleteTableStatement);
        generalPage.closeStatementAndConnection();
    }

    @Test(dependsOnMethods = "dropTable")
    public void dropDatabase() throws SQLException {
        generalPage.setupConnection("", user, password);
        String sqlDeleteTableStatement = "DROP DATABASE `vehicles`";
        generalPage.dropDatabase("vehicles", sqlDeleteTableStatement);
        assertFalse(generalPage.dropDatabase("vehicles", sqlDeleteTableStatement)
                .contains("vehicle"), "Checking that vehicle schema was really deleted");
        System.out.println("Schema is really deleted");
        generalPage.closeStatementAndConnection();
    }
}
