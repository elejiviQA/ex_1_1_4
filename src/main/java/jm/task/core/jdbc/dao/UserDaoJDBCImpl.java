package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {

        Connection connection = null;
        String createUsersTable = """
                CREATE TABLE IF NOT EXISTS users(
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(60) NOT NULL,
                lastName VARCHAR(60) NOT NULL,
                age TINYINT UNSIGNED NOT NULL
                );
                """;

        try {
            connection = Util.get();
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            statement.executeUpdate(createUsersTable);
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            // try { connection.rollback(); } catch (SQLException ex) { throw new RuntimeException(ex); } //rollback for DDL?
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void dropUsersTable() {

        Connection connection = null;
        String dropUsersTable = """
                DROP TABLE IF EXISTS users;
                """;

        try {
            connection = Util.get();
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            statement.executeUpdate(dropUsersTable);
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            // try { connection.rollback(); } catch (SQLException ex) { throw new RuntimeException(ex); } //rollback for DDL?
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {

        String saveUser = String.format("INSERT INTO users (name, lastName, age) VALUES ('%s', '%s', '%d');", name, lastName, age);
        String result = String.format("User с именем – '%s' добавлен в базу данных", name);
        Connection connection = null;

        try {
            connection = Util.get();
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            statement.executeUpdate(saveUser);
            connection.commit();
            connection.setAutoCommit(true);
            System.out.println(result);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void removeUserById(long id) {

        String removeUserById = String.format("DELETE FROM users WHERE id = '%d'", id);
        Connection connection = null;

        try {
            connection = Util.get();
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            statement.executeUpdate(removeUserById);
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<User> getAllUsers() {

        String getAllUsers = "SELECT * FROM users";
        List<User> users = new ArrayList<>();
        Connection connection = null;

        try {
            connection = Util.get();
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            var resultExecute = statement.executeQuery(getAllUsers);
            while (resultExecute.next()) {
                User user = new User();
                user.setId(resultExecute.getLong("id"));
                user.setName(resultExecute.getString("name"));
                user.setLastName(resultExecute.getString("lastName"));
                user.setAge(resultExecute.getByte("age"));
                users.add(user);
            }

            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return users;
    }

    public void cleanUsersTable() {

        String cleanUsersTable = "DELETE FROM live.users;";
        Connection connection = null;

        try {
            connection = Util.get();
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            statement.executeUpdate(cleanUsersTable);
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
