package jdbc;

import org.apache.commons.dbutils.QueryRunner;
import org.junit.After;
import org.junit.Before;

import java.sql.*;

public class TimestampTest {

    private static final String TABLE_NAME = "tstest";
    Connection cn;
    QueryRunner runner;

    @Before
    public void init() throws SQLException {
        cn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test");
        runner = new QueryRunner();
    }

    // https://issues.apache.org/jira/browse/ROL-2095
    @org.junit.Test(expected = SQLException.class)
    public void ROL_2095() throws Exception {
        dropTableIfExists();
        createTable();
        insertRowContainsTimestamp();
        selectAsTimestamp();
        modifyTimestampToDatetime();
        selectAsTimestamp(); // should produce error? didn't happen with MySQL 5.6.27
    }

    private void dropTableIfExists() throws SQLException {
        runner.update(cn, "drop table if exists " + TABLE_NAME);
    }

    private void createTable() throws SQLException {
        runner.update(cn, "create table " + TABLE_NAME + " (col1 timestamp)");
    }

    private void insertRowContainsTimestamp() throws SQLException {
        runner.update(cn, "insert into " + TABLE_NAME + " values (CURRENT_TIMESTAMP)");
    }

    private void modifyTimestampToDatetime() throws SQLException {
        runner.update(cn, "alter table " + TABLE_NAME + " modify col1 datetime(3)");
    }

    private void selectAsTimestamp() throws SQLException {
        try (Statement st = cn.createStatement();
             ResultSet rs = st.executeQuery("select * from " + TABLE_NAME)) {
            while (rs.next()) {
                System.out.println(rs.getTimestamp(1));
            }
        }
    }

    @After
    public void close() throws SQLException {
        cn.close();
    }
}
