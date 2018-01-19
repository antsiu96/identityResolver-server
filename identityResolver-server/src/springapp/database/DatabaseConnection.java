package springapp.database;

import java.sql.Connection;
import java.sql.DriverManager;

public abstract class DatabaseConnection {
    
    protected Connection getConnection() throws Exception {
        return DriverManager
                .getConnection("jdbc:mysql://localhost/lab?"
                        + "user=root&password=anthony96");        
    }
}