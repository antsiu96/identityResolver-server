package springapp.location.device.controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeviceController {
    //TODO should break up location tables by region
    
    @RequestMapping(value = "/device", method = RequestMethod.PUT)
    public void update(
            @RequestParam(value="id") String id,
            @RequestParam(value="userId") String userId,
            @RequestParam(value="longitude") String longitude,
            @RequestParam(value="latitude") String latitude)
                    throws Exception {
        
        System.out.println(String.format("id: %s | userId: %s | longitude: %s | latitude: %s", id, userId, longitude, latitude));
        
        //TODO input validation regex?
        
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            
            preparedStatement = connection.prepareStatement(
                    "UPDATE device"
                    + " SET location = ST_GeomFromText('POINT(" + longitude + " " + latitude +")')," //TODO parameterize prepare with spatial data?
                    + " userId = ?,"
                    + " updateTime = NOW()"
                    + " WHERE id = ?");
            preparedStatement.setString(1, userId);            
            preparedStatement.setString(2, id);
            
            preparedStatement.executeUpdate();
            
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }        
    }
    
    @RequestMapping(value = "/device", method = RequestMethod.POST)
    public void insert(
            @RequestParam(value="id") String id,
            @RequestParam(value="userId") String userId,
            @RequestParam(value="longitude") String longitude,
            @RequestParam(value="latitude") String latitude)
                    throws Exception {
        
        System.out.println(String.format("id: %s | userId: %s | longitude: %s | latitude: %s", id, userId, longitude, latitude));
        
        //TODO input validation regex?
        
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            
            preparedStatement = connection.prepareStatement(
                    "INSERT INTO device values("
                    + " ?,"
                    + " ?,"
                    + " ST_GeomFromText('POINT(" + longitude + " " + latitude +")')," //TODO parameterize prepare with spatial data?
                    + " NOW())");
            preparedStatement.setString(1, id);            
            preparedStatement.setString(2, userId);
            
            preparedStatement.executeUpdate();
            
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }        
    }    
    
    @RequestMapping(value = "/device", method = RequestMethod.GET)
    public ResponseEntity<Void> idExist(
            @RequestParam(value="id") String id)
                throws Exception {
        
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            
            preparedStatement = connection.prepareStatement(
                    "SELECT EXISTS(SELECT 1 FROM device"
                    + " WHERE id = ?)");            
            preparedStatement.setString(1, id);
            
            int count = 0;
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getInt(1);
            }
            
            return count == 0 ? new ResponseEntity<Void>(HttpStatus.NOT_FOUND) : new ResponseEntity<Void>(HttpStatus.OK);
            
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }         
    }
    
    public void createTable() throws Exception {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection();
            
            statement = connection.createStatement();
            statement.executeUpdate(
                    "CREATE TABLE device ("
                    + " id VARCHAR(30) NOT NULL,"
                    + " userId VARCHAR(30) NOT NULL,"
                    + " location GEOMETRY,"
                    + " updateTime DATETIME NOT NULL,"
                    + " PRIMARY KEY (id))");
            
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }
    
    private Connection getConnection() throws Exception {
        return DriverManager
                .getConnection("jdbc:mysql://localhost/lab?"
                        + "user=root&password=anthony96");        
    }
    
    public static void main(String[] args) throws Exception {
        new DeviceController().createTable();
    }
}