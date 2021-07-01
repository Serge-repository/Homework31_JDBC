package tests;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.*;

public class DataBaseManipulations {
    static String user = "root";
    static String password = "Ds8845";

    public void createDatabase() {
        String databaseURL = "jdbc:mysql://localhost:3306/";
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(databaseURL, user, password);
            System.out.println("Connection driver created");
            statement = connection.createStatement();
            System.out.println("Statement request is ready for execution");
            String sqlRequest = "CREATE DATABASE VEHICLES";
            statement.execute(sqlRequest);
            System.out.println("Schema created");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }
    }

    public void createTable() {
        String databaseLink = "jdbc:mysql://localhost:3306/VEHICLES";
        Connection connectionForNewTable = null;
        Statement statementForNewTable = null;
        try {
            connectionForNewTable = DriverManager.getConnection(databaseLink, user, password);
            System.out.println("Connection driver created");
            statementForNewTable = connectionForNewTable.createStatement();
            System.out.println("Statement request is ready for execution");
            String sqlRequestCreateTable = "CREATE TABLE CARS " +
                    "(id INTEGER not NULL, " +
                    " speed VARCHAR(255), " +
                    " colour VARCHAR(255), " +
                    " year INTEGER, " +
                    " PRIMARY KEY ( id ))";
            statementForNewTable.executeUpdate(sqlRequestCreateTable);
            System.out.println("Table created");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connectionForNewTable != null) {
                try {
                    connectionForNewTable.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statementForNewTable != null) {
                try {
                    statementForNewTable.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void insertIntoTable() {
        String insertDatabaseLink = "jdbc:mysql://localhost:3306/VEHICLES?user=root&password=Ds8845&serverTimezone=UTC";
        Connection connectionForNewRow = null;
        Statement statementForNewRow = null;
        try {
            connectionForNewRow = DriverManager.getConnection(insertDatabaseLink);
            System.out.println("Connection driver created");
            if (connectionForNewRow != null) {
                System.out.println("Connected to the database cars");
                statementForNewRow = connectionForNewRow.createStatement();
            }
            String sqlInsertStatement = "INSERT INTO `vehicles`.`cars` (`id`, `speed`, `colour`, `year`) \n" +
                    "VALUES ('1', '300', 'black', 1998), ('2', '320', 'yellow', 2000)";
            if (statementForNewRow != null) {
                statementForNewRow.execute(sqlInsertStatement);
            }
            System.out.println("New rows are created");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (connectionForNewRow != null) {
                try {
                    connectionForNewRow.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statementForNewRow != null) {
                try {
                    statementForNewRow.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void assertAddedInformation() throws SQLException {
        String selectLink = "jdbc:mysql://localhost:3306/VEHICLES?user=root&password=Ds8845&serverTimezone=UTC";
        Connection selectConnection;
        Statement selectStatement = null;
        ResultSet resultSet = null;

        selectConnection = DriverManager.getConnection(selectLink);
        System.out.println("Connection as driver set");

        if (selectConnection != null) {
            selectStatement = selectConnection.createStatement();
            System.out.println("Statement created");
        }
        if (selectStatement != null) {
            resultSet = selectStatement.executeQuery("select * from vehicles.cars");
            System.out.println("Execute query");
        }
        List<String[]> allRows = new ArrayList<String[]>();
        int numberColumns = 4;
        if (resultSet != null) {
            while (resultSet.next()) {
                String[] currentRow = new String[numberColumns];
                for (int i = 1; i <= numberColumns; i++) {
                    currentRow[i - 1] = resultSet.getString(i);
                }
                System.out.println();
                allRows.add(currentRow);
            }
        }
        assertTrue(Arrays.toString(allRows.get(1)).contains("yellow"), "Second added row is yellow car");
        System.out.println("Assertion passed - car is yellow");
    }

    public void updateTable() throws SQLException {
        String insertDatabaseLink = "jdbc:mysql://localhost:3306/VEHICLES?user=root&password=Ds8845&serverTimezone=UTC";
        Connection connectionForNewRow = null;
        Statement statementForNewRow = null;
        try {
            connectionForNewRow = DriverManager.getConnection(insertDatabaseLink);
            System.out.println("Connection driver created");
            if (connectionForNewRow != null) {
                System.out.println("Connected to the database cars");
                statementForNewRow = connectionForNewRow.createStatement();
            }
            String sqlInsertStatement = "UPDATE `vehicles`.`cars` " +
                    "SET `colour` = 'orange' " +
                    "WHERE (`id` = '2')";
            if (statementForNewRow != null) {
                statementForNewRow.execute(sqlInsertStatement);
            }
            System.out.println("Row 2 is updated. Second car is now orange");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (connectionForNewRow != null) {
                connectionForNewRow.close();
            }
            if (statementForNewRow != null) {
                statementForNewRow.close();
            }
        }
    }

    public void assertAddedCarIsOrange() throws SQLException {
        String selectLink = "jdbc:mysql://localhost:3306/VEHICLES?user=root&password=Ds8845&serverTimezone=UTC";
        Connection selectConnection;
        Statement selectStatement = null;
        ResultSet resultSet = null;

        selectConnection = DriverManager.getConnection(selectLink);
        System.out.println("Connection as driver set");

        if (selectConnection != null) {
            selectStatement = selectConnection.createStatement();
            System.out.println("Statement created");
        }
        if (selectStatement != null) {
            resultSet = selectStatement.executeQuery("select * from vehicles.cars");
            System.out.println("Execute query");
        }
        List<String[]> allRows = new ArrayList<String[]>();
        int numberColumns = 4;
        if (resultSet != null) {
            while (resultSet.next()) {
                String[] currentRow = new String[numberColumns];
                for (int i = 1; i <= numberColumns; i++) {
                    currentRow[i - 1] = resultSet.getString(i);
                }
                System.out.println();
                allRows.add(currentRow);
            }
        }
        assertTrue(Arrays.toString(allRows.get(1)).contains("orange"), "Second added row is orange car");
        System.out.println("Assertion passed - car is orange");
    }

    public void deleteFromTable() throws SQLException {
        String insertDatabaseLink = "jdbc:mysql://localhost:3306/VEHICLES?user=root&password=Ds8845&serverTimezone=UTC";
        Connection connectionForNewRow = null;
        Statement statementForNewRow = null;
        try {
            connectionForNewRow = DriverManager.getConnection(insertDatabaseLink);
            System.out.println("Connection driver created");
            if (connectionForNewRow != null) {
                System.out.println("Connected to the database cars");
                statementForNewRow = connectionForNewRow.createStatement();
            }
            String sqlInsertStatement = "DELETE FROM `vehicles`.`cars` WHERE (`id` = '2')";
            if (statementForNewRow != null) {
                statementForNewRow.execute(sqlInsertStatement);
            }
            System.out.println("Row 2 is deleted");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (connectionForNewRow != null) {
                connectionForNewRow.close();
            }
            if (statementForNewRow != null) {
                statementForNewRow.close();
            }
        }
    }

    public void assertRowDeleted() throws SQLException {
        String selectLink = "jdbc:mysql://localhost:3306/VEHICLES?user=root&password=Ds8845&serverTimezone=UTC";
        Connection selectConnection;
        Statement selectStatement = null;
        ResultSet resultSet = null;

        selectConnection = DriverManager.getConnection(selectLink);
        System.out.println("Connection as driver is set");

        if (selectConnection != null) {
            selectStatement = selectConnection.createStatement();
            System.out.println("Statement created");
        }
        if (selectStatement != null) {
            resultSet = selectStatement.executeQuery("select * from vehicles.cars");
            System.out.println("Statement executed");
        }
        List<String[]> allRows = new ArrayList<String[]>();
        int numberColumns = 4;
        if (resultSet != null) {
            while (resultSet.next()) {
                String[] currentRow = new String[numberColumns];
                for (int i = 1; i <= numberColumns; i++) {
                    currentRow[i - 1] = resultSet.getString(i);
                }
                allRows.add(currentRow);
            }
        }
        assertEquals(allRows.size(), 1, "Table contains only one row");
        System.out.println("Table contains only one row");
    }

    public void dropTable() throws SQLException {
        String dropTableLink = "jdbc:mysql://localhost:3306/VEHICLES?user=root&password=Ds8845&serverTimezone=UTC";
        Connection connectionForDeleteTable = null;
        Statement statementForTableDelete = null;
        try {
            connectionForDeleteTable = DriverManager.getConnection(dropTableLink);
            System.out.println("Connection driver created");
            if (connectionForDeleteTable != null) {
                statementForTableDelete = connectionForDeleteTable.createStatement();
                System.out.println("Statement is ready");
            }

            DatabaseMetaData metadata = connectionForDeleteTable.getMetaData();
            ResultSet resultSet = metadata.getTables(null, null, "cars", null);
            while (resultSet.next()) {
                if (resultSet.getString(3).equals("cars")) {
                    System.out.println("Table " + resultSet.getString(3) + " exists and will be deleted");
                    String sqlInsertStatement = "DROP TABLE `vehicles`.`cars`";
                    if (statementForTableDelete != null) {
                        statementForTableDelete.execute(sqlInsertStatement);
                    }
                } else {
                    throw new SQLException("Table doesn`t exists. Nothing to delete");
                }
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (connectionForDeleteTable != null) {
                connectionForDeleteTable.close();
            }
            if (statementForTableDelete != null) {
                statementForTableDelete.close();
            }
        }
    }

    public void dropDatabase() throws SQLException {
        String dropDatabaseLink = "jdbc:mysql://localhost:3306";
        Connection connectionForDropDatabase = null;
        Statement statementForDropDatabase = null;
        try {
            connectionForDropDatabase = DriverManager.getConnection(dropDatabaseLink, user, password);
            System.out.println("Connection driver created");
            if (connectionForDropDatabase != null) {
                statementForDropDatabase = connectionForDropDatabase.createStatement();
                System.out.println("Statement is ready");
            }

            DatabaseMetaData metadata = null;
            if (connectionForDropDatabase != null) {
                metadata = connectionForDropDatabase.getMetaData();
            }
            ResultSet resultSet = null;
            if (metadata != null) {
                resultSet = metadata.getCatalogs();
            }
            ArrayList<String> requiredSchemaNameChecker = new ArrayList<String>();
            while (resultSet.next()) {
                if (resultSet.getString("TABLE_CAT").equals("vehicles")) {
                    System.out.println("Schema " + resultSet.getString("TABLE_CAT") + " exists and will be deleted");
                    String sqlInsertStatement = "DROP DATABASE `vehicles`";
                    if (statementForDropDatabase != null) {
                        statementForDropDatabase.execute(sqlInsertStatement);
                        System.out.println("Schema deleted successfuly");
                        requiredSchemaNameChecker.add(resultSet.getString("TABLE_CAT"));
                    }
                }
            }

            assertFalse(requiredSchemaNameChecker.contains("vehicle"), "Checking that vehicle schema was really deleted");
            System.out.println("Schema is really deleted");

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (connectionForDropDatabase != null) {
                connectionForDropDatabase.close();
            }
            if (statementForDropDatabase != null) {
                statementForDropDatabase.close();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        DataBaseManipulations dataBaseManipulations = new DataBaseManipulations();
        dataBaseManipulations.createDatabase();
        dataBaseManipulations.createTable();
        dataBaseManipulations.insertIntoTable();
        dataBaseManipulations.assertAddedInformation();
        dataBaseManipulations.updateTable();
        dataBaseManipulations.assertAddedCarIsOrange();
        dataBaseManipulations.deleteFromTable();
        dataBaseManipulations.assertRowDeleted();
        dataBaseManipulations.dropTable();
        dataBaseManipulations.dropDatabase();
    }
}