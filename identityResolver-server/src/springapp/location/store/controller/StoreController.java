package springapp.location.store.controller;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import springapp.database.DatabaseConnection;

@RestController
public class StoreController extends DatabaseConnection {
    //TODO should break up store tables by region
    
    @RequestMapping(value = "/store", method = RequestMethod.PUT)
    public void update(
            @RequestParam(value="id") String id,
            @RequestParam(value="points") String[] points)
                    throws Exception {
        
        //TODO input validation regex?
        
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            
            preparedStatement = connection.prepareStatement(
                    "UPDATE store"
                    + " SET location = ST_GeomFromText('POLYGON((" + pointsParam(points) +"))')," //TODO parameterize prepare with spatial data?
                    + " WHERE id = ?");
            preparedStatement.setString(1, id);
            
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
    
    @RequestMapping(value = "/store", method = RequestMethod.POST)
    public void insert(
            @RequestParam(value="id") String id,
            @RequestParam(value="points") String[] points)
                    throws Exception {
        
        //TODO input validation regex?
        
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();            
            preparedStatement = connection.prepareStatement(
                    "INSERT INTO store values("
                    + " ?,"
                    + " ST_GeomFromText('POLYGON((" + pointsParam(points) +"))'))"); //TODO parameterize prepare with spatial data?
            preparedStatement.setString(1, id);
            
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
    
    @RequestMapping(value = "/store/users", method = RequestMethod.GET)
    public ResponseEntity<String> getUsers(
            @RequestParam(value="id") String id)
                    throws Exception {
        
        //TODO input validation regex?
        
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = null;
        PreparedStatement preparedStatement = null;      
        try {
            connection = getConnection();
            
            preparedStatement = connection.prepareStatement(
                    "SELECT location FROM store"
                    + " WHERE id = ?");
            preparedStatement.setString(1, id);
            
            Object polygon = null;
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                polygon = resultSet.getObject(1);
            }
            
            return polygon == null ? new ResponseEntity<String>("Failed", HttpStatus.NOT_FOUND) : new ResponseEntity<String>(listToString(getUsersList(connection, polygon)), HttpStatus.OK);
            
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }        
    }
    
    long min = 30;
    
    public ArrayList<String> getUsersList(Connection connection, Object polygon) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        PreparedStatement preparedStatement = null; 
        try {
            connection = getConnection();
        
            preparedStatement = connection.prepareStatement(
                    "SELECT userId FROM device"
                    + " WHERE ST_CONTAINS(?, location)"
                    + " AND updateTime >= ?");
            preparedStatement.setObject(1, polygon);
            preparedStatement.setDate(2, new Date(System.currentTimeMillis()-min*60000));             
            
            ArrayList<String> users = new ArrayList<String>();
            ResultSet resultSet = preparedStatement.executeQuery();            
            while (resultSet.next()) {
                users.add(resultSet.getString(1));
            }
            
            return users;  
            
        } finally {
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
                    "CREATE TABLE store ("
                    + " id VARCHAR(30) NOT NULL,"
                    + " location GEOMETRY,"
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
    
    //TODO Util library?
    protected String listToString(List<String> list) throws Exception {
        StringBuilder string = new StringBuilder();
        for( String item : list ){
            string.append(item).append(" ");
        }
        return string.toString().trim();
    }
    
    protected String pointsParam(String[] points) throws Exception {
        if (points == null || points.length < 3) {
            throw new Exception("Not enough points provided");
        }
        StringBuilder pointsParam = new StringBuilder();
        pointsParam.append(points[0]);
        for (int i = 1; i < points.length; i++) {
            pointsParam.append(",").append(points[i]);
        }
        pointsParam.append(",").append(points[0]); //Close polygon
        
        return pointsParam.toString();
    }
    
    public static void main(String[] args) throws Exception {
        new StoreController().createTable();
    }
}