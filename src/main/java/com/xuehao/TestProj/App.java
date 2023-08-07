package com.xuehao.TestProj;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Random;
import javax.sql.DataSource;

import com.zaxxer.hikari.*;

/**
 * Hello world! 这是一个模拟多服务器并发访问数据库的程序 想要实现的或者模拟的功能，
 * 1.每个客户端进入时，读取最新的一条数据，都向数据库新增一条数据。接着下一个客户端读取最后一条新增的数据。
 * 2.管理客户端进入时，根据学生们的成绩给学生进行评级，90-100A，75-90B，60-75C，其余D(新建column)
 * 
 * 运行前需要先打开mysql服务器。 需要解决的问题： 1. 报错：HikariPool-1 - Connection is not available,
 * request timed out after 30001ms.
 * 我已经定义了连接池的最大连接数为20，但是开启的线程只有10个，也就是十个客户端去访问，但是还是会报这样的错误，修改了最大连接数也没有用，问题出在哪里？
 * 问题解决：因为connection的关闭位置不对，要在try-catch内部关闭连接，或者直接采用try(resources)的方法。
 * 更推荐采用try(resources)的方法，时间更快，使用手动close的方法效率太低.
 */
public class App {

	public static void main(String[] args) throws SQLException, InterruptedException {
		run();
	}

	static void run() throws InterruptedException {
		List<Thread> threadList = new ArrayList<Thread>();
		SQLExecuter sqlExecuter = new SQLExecuter();
		// 写任务
		Runnable insertLineTask = new Runnable() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				String[] argStrings = DataGenerator.generate();
				sqlExecuter.insertLine(argStrings);
			}
		};
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			Thread thread = new Thread(() -> {
				// 先插入一行数据，在进行查询
				String[] insertString = DataGenerator.generate();
				System.out.println("insert data: " + Arrays.toString(insertString));
				sqlExecuter.insertLine(insertString);
			});
			thread.setName(String.format("Thread->%d", i));

			threadList.add(thread);
			thread.start();
			System.out.println(String.format("tread[%d] start", i));
		}

		for (Thread thread : threadList) {
			thread.join();
		}
		
		String string = "select * from students";
		sqlExecuter.queryAll(string).forEach(System.out::println);
		
		long end = System.currentTimeMillis();
		System.out.println(String.format("All thread is execute over,execute time:%dms", end - start));
	}

}

class SQLExecuter {
	private String JDBC_URL = "jdbc:mysql://localhost:3306/learnjdbc?useSSL=false&characterEncoding=utf8";
	private String JDBC_USER = "root";
	private String JDBC_PASSWORD = "password";

	private static HikariConfig config = new HikariConfig();

	private static DataSource ds;

	public String getJDBC_URL() {
		return JDBC_URL;
	}

	public void setJDBC_URL(String jDBC_URL) {
		JDBC_URL = jDBC_URL;
	}

	public String getJDBC_USER() {
		return JDBC_USER;
	}

	public void setJDBC_USER(String jDBC_USER) {
		JDBC_USER = jDBC_USER;
	}

	public String getJDBC_PASSWORD() {
		return JDBC_PASSWORD;
	}

	public void setJDBC_PASSWORD(String jDBC_PASSWORD) {
		JDBC_PASSWORD = jDBC_PASSWORD;
	}

	public HikariConfig getConfig() {
		return config;
	}

	public DataSource getDs() {
		return ds;
	}

	public SQLExecuter() {
		config.setJdbcUrl(JDBC_URL);
		config.setUsername(JDBC_USER);
		config.setPassword(JDBC_PASSWORD);
		config.addDataSourceProperty("connectionTimeout", "10000"); // 连接超时：10秒
		config.addDataSourceProperty("idleTimeout", "60000"); // 空闲超时：60秒
		config.addDataSourceProperty("maximumPoolSize", "100"); // 最大连接数：100
		ds = new HikariDataSource(config);
		// TODO Auto-generated constructor stub
	}

	public SQLExecuter(String JDBC_URL, String JDBC_USER, String JDBC_PASSWORD) {
		config.setJdbcUrl(JDBC_URL);
		config.setUsername(JDBC_USER);
		config.setPassword(JDBC_PASSWORD);
		config.addDataSourceProperty("connectionTimeout", "1000"); // 连接超时：1秒
		config.addDataSourceProperty("idleTimeout", "60000"); // 空闲超时：60秒
		config.addDataSourceProperty("maximumPoolSize", "100"); // 最大连接数：10
		ds = new HikariDataSource(config);
		// TODO Auto-generated constructor stub
	}

	public List<Student> queryAll() {
		List<Student> stdList = new ArrayList<Student>();
		String queryString = "SELECT * FROM students;";
		try (Connection connection = ds.getConnection()) {
			try (PreparedStatement pStatement = connection.prepareStatement(queryString)) {
				ResultSet rSet = pStatement.executeQuery();
				while (rSet.next()) {
					stdList.add(extractRow(rSet));
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		// 关闭connection
		return stdList;
	}

	public List<Student> queryAll(String queryString) {
		List<Student> stdList = new ArrayList<Student>();
		try (Connection connection = ds.getConnection()) {
			try (PreparedStatement pStatement = connection.prepareStatement(queryString)) {
				ResultSet rSet = pStatement.executeQuery();
				while (rSet.next()) {
					stdList.add(extractRow(rSet));
				}
				// 关闭connection
			} catch (Exception e) {
				// TODO: handle exception
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return stdList;
	}

	public void showQuery() {
		System.out.println(String.format("I'm %s", Thread.currentThread().getName()));

		queryAll().forEach(System.out::println);
	}

	public void insertLines(String[]... args) throws SQLException {
		// 插入sql
		String[][] values = args.clone();
		String insertString = "REPLACE INTO students (id,name,gender,grade,score) VALUES (?,?,?,?,?)";
		try (Connection connection = ds.getConnection()) {
			try (PreparedStatement pStatement = connection.prepareStatement(insertString,
					Statement.RETURN_GENERATED_KEYS)) {
				for (String[] strings : values) {
					pStatement.setInt(1, Integer.parseInt(strings[0]));
					pStatement.setString(2, strings[1]);
					pStatement.setInt(3, Integer.parseInt(strings[2]));
					pStatement.setInt(4, Integer.parseInt(strings[3]));
					pStatement.setInt(5, Integer.parseInt(strings[4]));
					pStatement.addBatch();
				}
				int[] n = pStatement.executeBatch();
				System.out.println("insert " + Arrays.toString(n) + " lines");
				try (ResultSet rs = pStatement.getGeneratedKeys()) {
					if (rs.next()) {
						long id = rs.getLong(1); // 注意：索引从1开始
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void insertLine(String... args) {
		// 插入sql
		String[] values = args.clone();
		String insertString = "INSERT INTO students (name,gender,grade,score) VALUES (?,?,?,?)";
		try (Connection connection = ds.getConnection()) {
			try (PreparedStatement pStatement = connection.prepareStatement(insertString,
					Statement.RETURN_GENERATED_KEYS)) {
				pStatement.setString(1, values[0]);
				pStatement.setInt(2, Integer.parseInt(values[1]));
				pStatement.setInt(3, Integer.parseInt(values[2]));
				pStatement.setInt(4, Integer.parseInt(values[3]));
				int n = pStatement.executeUpdate();
				System.out.println("insert " + n + " lines");
				try (ResultSet rSet = pStatement.getGeneratedKeys()) {
					long id = rSet.getLong(1);
					Thread.sleep(10);
					System.out.println(id);
				} catch (Exception e) {
					// TODO: handle exception
					System.out.println(e.toString());
				}
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println(e.toString());
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.toString());
		}

	}

	public void deleteByID(int ID) throws SQLException {
		try (Connection connection = ds.getConnection()) {
			try (PreparedStatement pStatement = connection.prepareStatement("DELETE FROM students WHERE id>=?",
					Statement.RETURN_GENERATED_KEYS)) {
				pStatement.setInt(1, ID);
				int n = pStatement.executeUpdate();
				System.out.println("delete " + n + " lines");
				try (ResultSet rSet = pStatement.getGeneratedKeys()) {
					long id = rSet.getLong(1);
					Thread.sleep(10);
					System.out.println(rSet.toString());
				} catch (Exception e) {
					// TODO: handle exception
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	static Student extractRow(ResultSet rs) throws SQLException {
		Student std = new Student();
		std.setId(rs.getLong("id"));
		std.setName(rs.getString("name"));
		std.setGender(rs.getBoolean("gender"));
		std.setGrade(rs.getInt("grade"));
		std.setScore(rs.getInt("score"));
		return std;
	}
}

/**
 * 数据库数据生成器 调用generate进行生成
 */
class DataGenerator {
	private static Random random = new Random();

	// 生成随机姓名
	public static String generateName() {
		String[] names = { "Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Henry", "Ivy", "Jack",
				"Ethan Thompson", "Olivia Parker", "Liam Anderson", "Ava Mitchell", "Noah Turner", "Emma Wright",
				"Lucas Collins", "Sophia Bailey", "Benjamin Russell", "Isabella Cooper", "Mason Bennett", "Mia Ward",
				"Alexander Phillips", "Charlotte Reed", "James Foster", "Harper Murphy", "Henry Ross", "Amelia Davis",
				"Samuel Brooks", "Grace Richardson" };
		return names[random.nextInt(names.length)];
	}

	// 生成随机性别（0：女性，1：男性）
	public static int generateGender() {
		return random.nextInt(2);
	}

	// 生成随机年级（1-6）
	public static int generateGrade() {
		return random.nextInt(6) + 1;
	}

	// 生成随机分数（0-100）
	public static int generateScore() {
		return random.nextInt(101);
	}

	/**
	 * generate serial data, include name,gender,grade and socre
	 * 
	 * @return String[]
	 */
	public static String[] generate() {
		String[] retStrings = new String[4];
		retStrings[0] = generateName();
		retStrings[1] = Integer.toString(generateGender());
		retStrings[2] = Integer.toString(generateGrade());
		retStrings[3] = Integer.toString(generateScore());
		return retStrings;
	}
}