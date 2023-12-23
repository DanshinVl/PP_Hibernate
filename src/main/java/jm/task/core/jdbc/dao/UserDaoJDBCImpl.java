package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl extends Util implements UserDao {
    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS USERS";
    private static final String INSERT_SQL = "INSERT INTO USERS(name, lastname, age) VALUES (?, ?, ?)";
    private static final String DELETE_SQL = "DELETE FROM USERS WHERE id = ?";
    private static final String SELECT_SQL = "SELECT * FROM USERS";
    private static final String TRUNCATE_SQL = "TRUNCATE TABLE USERS";
    private final String createUsersSQL =
            "CREATE TABLE IF NOT EXISTS USERS" +
            "(id BIGINT not NULL AUTO_INCREMENT, " +
            " name VARCHAR(255), " +
            " lastname VARCHAR(255), " +
            " age TINYINT, " +
            " PRIMARY KEY ( id ))";
    private final Connection connection;

    public UserDaoJDBCImpl(Util util) {
        this.connection = util.getConnection();
    }

    public void createUsersTable() { //Создание таблицы для User(ов) – не должно приводить к исключению, если такая таблица уже существует
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(createUsersSQL);
        } catch (SQLException sqlEx) {
            throw new RuntimeException("Error with createUsersTable: " + sqlEx.getMessage(), sqlEx);
        }
    }

    public void dropUsersTable() {
        try {
            connection.setAutoCommit(false);

            try (PreparedStatement dropTable = connection.prepareStatement(
                    String.format(DROP_TABLE_SQL))) {
                dropTable.executeUpdate();
            } catch (SQLException ex) {
                connection.rollback();
                throw new RuntimeException("Error dropping users table:  " + ex.getMessage());
            }

            connection.commit();
        } catch (SQLException ex) {
            throw new RuntimeException("Error dropping users table: " + ex.getMessage());
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException ex) {
                throw new RuntimeException("Error dropping users table: " + ex.getMessage());
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
            ps.setString(1, name);
            ps.setString(2, lastName);
            ps.setByte(3, age);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Error saving user: " + name, e);
        }
    }

    public void removeUserById(long id) { //Удаление User из таблицы ( по id )
        try (PreparedStatement ps = connection.prepareStatement(DELETE_SQL)) {
            ps.setLong(1, id);
            int rowsDeleted = ps.executeUpdate();
            if (rowsDeleted == 0) {
                throw new RuntimeException("User with ID " + id + " does not exist");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error removing user with ID: " + id, e);
        }
    }

    public List<User> getAllUsers() {  //Получение всех User(ов) из таблицы
        List<User> userList = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(SELECT_SQL);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                User user = new User();
                user.setId(rs.getLong("id"));
                user.setName(rs.getString("name"));
                user.setLastName(rs.getString("lastname"));
                user.setAge(rs.getByte("age"));
                userList.add(user);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error retrieving all users: " + e.getMessage(), e);
        }
        return userList;
    }

    public void cleanUsersTable() {
        try {
            connection.setAutoCommit(false);

            try (PreparedStatement truncateStatement = connection.prepareStatement(TRUNCATE_SQL)) {
                truncateStatement.executeUpdate();
                connection.commit();
            } catch (SQLException sqlEx) {
                connection.rollback();
                throw new RuntimeException("Error cleaning users table: " + sqlEx.getMessage(), sqlEx);
            }
        } catch (SQLException connEx) {
            throw new RuntimeException("Error managing transaction: " + connEx.getMessage(), connEx);
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException autoCommitEx) {
                throw new RuntimeException("Error resetting auto-commit: " + autoCommitEx.getMessage(), autoCommitEx);
            }
        }
    }
}
