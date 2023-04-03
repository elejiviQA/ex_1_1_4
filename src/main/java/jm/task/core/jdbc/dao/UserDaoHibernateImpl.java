package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import java.util.List;

public class UserDaoHibernateImpl implements UserDao {

    public UserDaoHibernateImpl() {

    }


    @Override
    public void createUsersTable() {

        try (var session = Util.getSessionFactory().openSession()) {

            var transaction = session.beginTransaction();

            session.createNativeQuery("""
                CREATE TABLE IF NOT EXISTS users(
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(60) NOT NULL,
                lastName VARCHAR(60) NOT NULL,
                age TINYINT UNSIGNED NOT NULL
                );""", User.class).executeUpdate();
            transaction.commit();
        }
    }

    @Override
    public void dropUsersTable() {

        try (var session = Util.getSessionFactory().openSession()) {

            var transaction = session.beginTransaction();
            session.createNativeQuery("""
                DROP TABLE IF EXISTS users;
                """, User.class).executeUpdate();
            transaction.commit();
        }

    }

    @Override
    public void saveUser(String name, String lastName, byte age) {

        var result = String.format("User с именем – '%s' добавлен в базу данных", name);

        try (var session = Util.getSessionFactory().openSession()) {

            var transaction = session.beginTransaction();
            session.persist(new User(name, lastName, age));
            transaction.commit();
            System.out.println(result);
        }
    }

    @Override
    public void removeUserById(long id) {

        try (var session = Util.getSessionFactory().openSession()) {
            var transaction = session.beginTransaction();

            session.remove(session.find(User.class, id));
            transaction.commit();
        }
    }

    @Override
    public List<User> getAllUsers() {

        try (var session = Util.getSessionFactory().openSession()) {

            var transaction = session.beginTransaction();

            var users = session.createQuery("from User", User.class).getResultList();
            transaction.commit();
            return users;
        }
    }

    @Override
    public void cleanUsersTable() {

        try (var session = Util.getSessionFactory().openSession()) {

            var transaction = session.beginTransaction();

            var users = session.createQuery("from User", User.class).getResultList();
            for (var user: users) {
                session.remove(user);
            }
            transaction.commit();
        }

    }
}
