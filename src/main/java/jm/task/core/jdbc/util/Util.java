package jm.task.core.jdbc.util;

import jm.task.core.jdbc.model.User;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class Util {

    private static final String URL_KEY = "db.url";
    private static final String USERNAME_KEY = "db.username";
    private static final String PASSWORD_KEY = "db.password";
    private static final String POOL_SIZE_KEY = "db.pool.size";
    private static final Integer DEFAULT_POOL_SIZE = 1;

    private static BlockingQueue<Connection> pool;
    private static List<Connection> sourceConnections;
    private static SessionFactory sessionFactory;

    static {

        initConnectionPool();

        initSessionFactory();
    }

    private Util () {

    }

    public static SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    private static void initSessionFactory() {

        var serviceRegistryBuilder = new StandardServiceRegistryBuilder();
        serviceRegistryBuilder
                .applySetting("hibernate-dialect", "org.hibernate.dialect.MySQLDialect")
                .applySetting("connection.driver_class", "com.mysql.cj.jdbc.Driver")
                .applySetting("hibernate.hbm2ddl.auto", "update")
                .applySetting("hibernate.connection.url", "jdbc:mysql://localhost:3306/live")
                .applySetting("hibernate.connection.username","root")
                .applySetting("hibernate.connection.password", "root")
                .applySetting("show_sql", "false")
                .applySetting("hibernate.format_sql", "true");

        var serviceRegistry = serviceRegistryBuilder.build();
        var metadataSources = new MetadataSources(serviceRegistry).addAnnotatedClass(User.class);
        var metadataBuilder = metadataSources.getMetadataBuilder();
        var metadata = metadataBuilder.build();

        sessionFactory = metadata.buildSessionFactory();
    }

    private static void initConnectionPool() {

        var poolSize = PropertiesUtil.get(POOL_SIZE_KEY);
        var size = poolSize == null ? DEFAULT_POOL_SIZE : Integer.parseInt(poolSize);

        pool = new ArrayBlockingQueue<>(size);
        sourceConnections = new ArrayList<>();

        for (int i = 0; i < size; i++) {

            var connection = launch();
            var proxyConnection = (Connection)
                    Proxy.newProxyInstance(Util.class.getClassLoader(), new Class[]{Connection.class},
                            (proxy, method, args) -> method.getName().equals("close")
                                    ? pool.add((Connection) proxy)
                                    : method.invoke(connection, args));
            pool.add(proxyConnection);
            sourceConnections.add(connection);
        }
    }

    public static Connection get() {

        try {
            return pool.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Connection launch() {

        try {
            return DriverManager.getConnection(
                    PropertiesUtil.get(URL_KEY),
                    PropertiesUtil.get(USERNAME_KEY),
                    PropertiesUtil.get(PASSWORD_KEY));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closePool() {

        try {
            for (var sourceConnection : sourceConnections) {
                sourceConnection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
