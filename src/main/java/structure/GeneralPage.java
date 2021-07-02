package structure;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GeneralPage {
    private final String databaseURL;
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private DatabaseMetaData databaseMetaData;

    public GeneralPage(String databaseURL, Connection connection, Statement statement, ResultSet resultSet) {
        this.databaseURL = databaseURL;
        this.connection = connection;
        this.statement = statement;
        this.resultSet = resultSet;
    }

    public void setupConnection(String databasePath, String user, String password) {
        String fullDatabasePath = databaseURL + databasePath;
        try {
            connection = DriverManager.getConnection(fullDatabasePath, user, password);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println("Connection driver created");
    }

    public List<String[]> getInfoFromAllRows(int numberOfColumns) throws SQLException {
        List<String[]> allRows = new ArrayList<String[]>();
        if (resultSet != null) {
            while (resultSet.next()) {
                String[] currentRow = new String[numberOfColumns];
                for (int i = 1; i <= numberOfColumns; i++) {
                    currentRow[i - 1] = resultSet.getString(i);
                }
                System.out.println();
                allRows.add(currentRow);
            }
        }
        return allRows;
    }

    public void setupAndExecuteStatement(String sqlStatement) {
        try {
            statement = connection.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println("Statement request is ready for execution");
        try {
            statement.execute(sqlStatement);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void createStatement(){
        try {
            statement = connection.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void setupResultSetAndExecuteStatement(String sqlStatement) throws SQLException {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sqlStatement);
    }

    public void closeStatementAndConnection(){
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try {
            statement.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void dropTable(String tableName, String sqlStatement) throws SQLException {
        databaseMetaData = connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getTables(null, null, tableName, null);
        while (resultSet.next()) {
            if (resultSet.getString(3).equals(tableName)) {
                System.out.println("Table " + resultSet.getString(3) + " exists and will be deleted");
                if (statement != null) {
                    statement.execute(sqlStatement);
                }
            } else {
                throw new SQLException("Table doesn`t exists. Nothing to delete");
            }
        }
    }

    public ArrayList<String> dropDatabase(String schemaName, String sqlStatement) throws SQLException {
        statement = connection.createStatement();
        databaseMetaData = connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getCatalogs();
        ArrayList<String> requiredSchemaNameChecker = new ArrayList<String>();
            while (resultSet.next()) {
                if (resultSet.getString("TABLE_CAT").equals(schemaName)) {

                    if (statement != null) {
                        statement.execute(sqlStatement);
                        System.out.println("Schema deleted successfuly");
                        requiredSchemaNameChecker.add(resultSet.getString("TABLE_CAT"));
                    }
                }
            }
            return requiredSchemaNameChecker;
    }
}
