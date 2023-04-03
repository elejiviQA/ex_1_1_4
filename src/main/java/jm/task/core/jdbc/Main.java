package jm.task.core.jdbc;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.service.UserServiceImpl;
import jm.task.core.jdbc.util.Util;

public class Main {
    public static void main(String[] args) {

        var userService = new UserServiceImpl();

        userService.createUsersTable();

        userService.saveUser("Nikita", "Kirilov", (byte) 28);
        userService.saveUser("Georgy", "Alburov", (byte) 27);
        userService.saveUser("Andrey", "Pavlov", (byte) 25);
        userService.saveUser("Ekaterina", "Morozova", (byte) 26);

        for (User user: userService.getAllUsers()) {
            System.out.println(user);
        }

        userService.cleanUsersTable();
        userService.dropUsersTable();

        Util.closePool();
    }
}
