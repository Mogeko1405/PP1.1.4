package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() throws SQLException {
        String query = """
                create table if not exists ppdb.users
                (
                id bigint auto_increment
                primary key,
                name varchar(45) null,
                lastname varchar(45) null,
                age tinyint null
                );""";
        Connection connection = null;
        try {
            connection = Util.getConnection();
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);
            statement.executeUpdate(query);
            connection.commit();
        } catch (SQLException e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        }
    }

    public void dropUsersTable() throws SQLException {
        String query = "DROP TABLE IF EXISTS users";
        Connection connection = null;
        try {
            connection = Util.getConnection();
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);
            statement.executeUpdate(query);
            connection.commit();
        } catch (SQLException e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        String query = "INSERT INTO users(`name`, lastname, age) VALUES (?, ?, ?)";
        Connection connection = null;
        try {
            connection = Util.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            connection.setAutoCommit(false);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.execute();
            connection.commit();
            System.out.println("User с именем - " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) throws SQLException {
        String query = "DELETE FROM users WHERE id = ?";
        Connection connection = null;
        try {
            connection = Util.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            connection.setAutoCommit(false);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String query = "SELECT * FROM users";
        try (PreparedStatement preparedStatement = Util.getConnection().prepareStatement(query)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            long loopId = 1;
            while (resultSet.next()) {
                User user = new User();
                user.setId(loopId++);
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                System.out.println(user);
                users.add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return users;
    }

    public void cleanUsersTable() throws SQLException {
        String query = "DELETE FROM users";
        Connection connection = null;
        try {
            connection = Util.getConnection();
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);
            statement.executeUpdate(query);
            connection.commit();
        } catch (SQLException e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
        }
    }
}
