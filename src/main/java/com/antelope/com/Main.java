package com.antelope.com;

import java.sql.*;

public class Main {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
	// write your code here
        Class.forName("com.mysql.cj.jdbc.Driver");

        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/poc",
                "root","rimpA@5536");
        Statement stmt=con.createStatement();
        ResultSet rs=stmt.executeQuery("select * from demo");
        while(rs.next())
            System.out.println(rs.getInt(1));
        con.close();
    }
}
