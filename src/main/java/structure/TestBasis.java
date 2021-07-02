package structure;

import org.testng.annotations.BeforeMethod;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestBasis {
    public String databaseURL;
    public Connection connection;
    public Statement statement;
    public ResultSet resultSet;

    protected final String user = "root";
    protected final String password = "******"; //password deleted in order to strenghten security

    public GeneralPage generalPage;

    @BeforeMethod
    public void actionsBeforeClass() {
        databaseURL = "jdbc:mysql://localhost:3306/";
        generalPage = new GeneralPage(databaseURL, connection, statement, resultSet);
    }
}