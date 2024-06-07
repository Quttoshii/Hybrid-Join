import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Scanner;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.text.ParseException;

//download "com.mysql.cj.jdbc.Driver" 
//  from https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.26

public class StreamGenerator extends Thread {
    String url;
    String username;
    String password;
    Connection connection;

    public StreamGenerator() {

        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter your database name: ");
        String db = scanner.nextLine();

        System.out.print("Enter your username: ");
        String un = scanner.nextLine();

        System.out.print("Enter your password: ");
        String pw = scanner.nextLine();

        scanner.close();
        this.url = "jdbc:mysql://localhost:3306/" + db;
        this.username = un;
        this.password = pw;
    }

    public ResultSet getData(String query) {
        ResultSet resultSet = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            this.connection = DriverManager.getConnection(url, username, password);

            Statement statement = connection.createStatement();

            resultSet = statement.executeQuery(query);

            return resultSet;
        } catch (Exception error) {
            System.out.println(error);
            return resultSet;
        }
    }

    public void insertData(Object[] values) {
        String insertDate = "INSERT IGNORE INTO D_Date (Day, Month, Quarter, Year)"
                + "VALUES (?, ?, ?, ?)";
        String insertOrder = "INSERT IGNORE INTO D_Order (OrderID, QuantityOrdered) VALUES (?, ?)";
        String insertStore = "INSERT IGNORE INTO D_Store (StoreID, StoreName) VALUES (?, ?)";
        String insertSupplier = "INSERT IGNORE INTO D_Supplier (SupplierID, SupplierName) VALUES (?, ?)";
        String insertCustomer = "INSERT IGNORE INTO D_Customer (CustomerID, CustomerName, Gender) VALUES (?, ?, ?)";
        String insertProduct = "INSERT IGNORE INTO D_Product (ProductID, ProductName, ProductPrice) VALUES (?, ?, ?)";
        String insertSales = "INSERT IGNORE INTO F_Sales (OrderID, DateID, ProductID, CustomerID, SupplierID"
                + ", StoreID, Total_Sale) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try {
            PreparedStatement statement = connection.prepareStatement(insertOrder);
            statement.setInt(1, (Integer) values[0]);
            statement.setInt(2, (Integer) values[6]);
            statement.execute();
        } catch (Exception e) {
            System.out.println(e);
        }

        try {
            PreparedStatement statement = connection.prepareStatement(insertStore);
            statement.setInt(1, (Integer) values[11]);
            statement.setString(2, (String) values[12]);
            statement.execute();
        } catch (Exception e) {
            System.out.println(e);
        }

        try {
            PreparedStatement statement = connection.prepareStatement(insertSupplier);
            statement.setInt(1, (Integer) values[9]);
            statement.setString(2, (String) values[10]);
            statement.execute();
        } catch (Exception e) {
            System.out.println(e);
        }

        try {
            PreparedStatement statement = connection.prepareStatement(insertCustomer);
            statement.setInt(1, (Integer) values[3]);
            statement.setString(2, (String) values[4]);
            statement.setString(3, (String) values[5]);
            statement.execute();
        } catch (Exception e) {
            System.out.println(e);
        }

        try {
            PreparedStatement statement = connection.prepareStatement(insertProduct);
            statement.setInt(1, (Integer) values[2]);
            statement.setString(2, (String) values[7]);
            statement.setDouble(3, ((BigDecimal) values[8]).doubleValue());
            statement.execute();
        } catch (Exception e) {
            System.out.println(e);
        }

        Integer dateId = dateInsertion(values[1], insertDate);

        try {
            PreparedStatement statement = connection.prepareStatement(insertSales);
            statement.setInt(1, (Integer) values[0]);
            statement.setInt(2, dateId);
            statement.setInt(3, (Integer) values[2]);
            statement.setInt(4, (Integer) values[3]);
            statement.setInt(5, (Integer) values[9]);
            statement.setInt(6, (Integer) values[11]);
            statement.setDouble(7, (Double) values[13]);
            statement.execute();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public Integer dateInsertion(Object val, String insertDate) {
        String input;
        if (val instanceof String) {
            input = ((String) val).split(" ")[0];
        } else if (val instanceof java.sql.Date) {
            input = val.toString().split(" ")[0];
        } else {
            throw new IllegalArgumentException("Unsupported date type: " + val.getClass());
        }
        String format = "yyyyMMdd";
        Integer week;
        SimpleDateFormat df = new SimpleDateFormat(format);
        String getLastInsertId = "SELECT LAST_INSERT_ID() AS LastID";
        Integer dateID = 0;

        try {
            Date date = df.parse(input);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            week = cal.get(Calendar.WEEK_OF_YEAR);

            String[] in = input.split("-");
            Integer quarter = 0;
            try {

                if (Integer.parseInt(in[1]) < 4) {
                    quarter = 1;
                } else if (Integer.parseInt(in[1]) > 3 && Integer.parseInt(in[1]) < 7) {
                    quarter = 2;
                } else if (Integer.parseInt(in[1]) > 6 && Integer.parseInt(in[1]) < 10) {
                    quarter = 3;
                } else {
                    quarter = 4;
                }
                try {
                    PreparedStatement statement = connection.prepareStatement(insertDate);
                    statement.setInt(1, Integer.parseInt(in[2]));
                    // statement.setInt(2, week);
                    statement.setInt(2, Integer.parseInt(in[1]));
                    statement.setInt(3, quarter);
                    statement.setInt(4, Integer.parseInt(in[0]));
                    statement.execute();

                    PreparedStatement lastIdStatement = connection.prepareStatement(getLastInsertId);
                    ResultSet resultSet = lastIdStatement.executeQuery();
                    resultSet.next();
                    dateID = resultSet.getInt("LastID");

                } catch (Exception e) {
                    System.out.println(e);
                }
            } catch (NumberFormatException e) {
                System.out.println(e);
            }
        } catch (ParseException e) {
            System.out.println(e);
        }
        return dateID;
    }

    @Override
    public void run() {

    }
}