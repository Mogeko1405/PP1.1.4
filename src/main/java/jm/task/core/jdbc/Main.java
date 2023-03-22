package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;



public class Main {
    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();
        userService.createUsersTable();
        userService.saveUser("A", "a", (byte) 11);
        userService.saveUser("B", "b", (byte) 12);
        userService.saveUser("C", "c", (byte) 13);
        userService.saveUser("D", "d", (byte) 14);
        userService.removeUserById(1);
        userService.getAllUsers();
        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
