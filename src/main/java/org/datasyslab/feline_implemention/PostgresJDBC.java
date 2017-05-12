package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import  java.sql. * ;

public class PostgresJDBC {
	
	public Connection con;
	public Statement st;
	public ResultSet rs;
	
	public PostgresJDBC()
	{
		con = GetConnection();
		
		try {
			st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ResultSet Execute(String query)
	{
		try
		{
			rs = st.executeQuery(query);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return rs;
		
	}
	
	public void DisConnect()
	{
		PostgresJDBC.Close(st);
		PostgresJDBC.Close(con);
	}

	public static Connection GetConnection()
	{
		Connection con = null;
		try
		{
			Class.forName( "org.postgresql.Driver" ).newInstance();
			String url = "jdbc:postgresql://localhost:5432/postgres" ;
			con = DriverManager.getConnection(url, "postgres" , "postgres" );   
		}
		catch (Exception ee)
		{
			ee.printStackTrace();
//			System.out.println("here");
//			System.out.println(ee.getMessage());
//			System.out.println(ee.getCause());
        }
		return con;
	}
	
	public static void Close(ResultSet resultSet) 
	{
	 
		if (resultSet == null)
			return;

		if (resultSet != null)
			try {
				resultSet.close();
			} catch (SQLException e) {
				/* Do some exception-logging here. */
				e.printStackTrace();
			}
	}
	
	public static void Close(Statement statement) 
	{

		if (statement == null)
			return;
		
		if (statement != null)
			try {
				statement.close();
			} catch (SQLException e) {
				/* Do some exception-logging here. */
				e.printStackTrace();
			}
	}
	
	public static void Close(Connection con)
	{
		if(con == null)
			return;
		
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static String StartServer(String password)
	{
		String []cmd = {"/bin/bash","-c","echo "+password+" | sudo -S sh -c \"/etc/init.d/postgresql start\""};
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(cmd);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();
	        result+="\n";
	        
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	public static String StopServer(String password)
	{
		String []cmd = {"/bin/bash","-c","echo "+password+" | sudo -S sh -c \"/etc/init.d/postgresql stop\""};
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(cmd);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();
	        result+="\n";
	        
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	public static void LoadData(String datasource, String suffix, String filesuffix)
	{
		File file = null;
		BufferedReader reader = null;
		Connection con = null;
		Statement st = null;
		try
		{
			con = PostgresJDBC.GetConnection();
			con.setAutoCommit(false);
			st = con.createStatement();
			//insert data
//			for(int ratio = 20;ratio<100;ratio+=20)
			
			int ratio = 20;
			{
				System.out.println("load "+datasource+"_Random_" + ratio + suffix);
				String filename = "/home/yuhansun/Documents/Real_data/"+datasource+"/"+filesuffix+"/" + ratio + "/entity.txt";
				file = new File(filename);
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				String tempString = null;
				while((tempString = reader.readLine())!=null)
				{
					String[] l = tempString.split(" ");
					int isspatial = Integer.parseInt(l[1]);
					if(isspatial == 0)
						continue;
					String tablename = datasource + "_Random_" + ratio + suffix;
					String query = "insert into " + tablename + " values (" + l[0] + ", '" + l[2] + "," + l[3] + "')";
					st.executeUpdate(query);
				}
				reader.close();
				con.commit();
			}
			st.close();
			con.setAutoCommit(true);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			PostgresJDBC.Close(st);
			PostgresJDBC.Close(con);
		}
	}
	
	public void LoadData(String datasource, String suffix, String filesuffix, int ratio)
	{
		File file = null;
		BufferedReader reader = null;
		try
		{
			con.setAutoCommit(false);
			{
				System.out.println("load "+datasource+"_Random_" + ratio + suffix);
				String filename = "/home/yuhansun/Documents/Real_data/"+datasource+"/"+filesuffix+"/" + ratio + "/entity.txt";
				file = new File(filename);
				reader = new BufferedReader(new FileReader(file));
				reader.readLine();
				String tempString = null;
				while((tempString = reader.readLine())!=null)
				{
					String[] l = tempString.split(" ");
					int isspatial = Integer.parseInt(l[1]);
					if(isspatial == 0)
						continue;
					String tablename = datasource + "_Random_" + ratio + suffix;
					String query = "insert into " + tablename + " values (" + l[0] + ", '" + l[2] + "," + l[3] + "')";
					st.executeUpdate(query);
				}
				reader.close();
				con.commit();
			}
			con.setAutoCommit(true);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void LoadDataByCopy(String table_name, String filesource_path, String delimiters)
	{
		try
		{
			String query = String.format("copy %s from '%s' using delimiters E'\t';", table_name, filesource_path, delimiters);
			st.executeUpdate(query);
		}
		catch(Exception e)
		{
			this.DisConnect();
			e.printStackTrace();
		}

	}
	
	public void CreateGistIndex(String datasource, String suffix, int ratio)
	{
		try
		{			
			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/gist_index_time.txt", true,"datasource\tratio\tconstruct_time\n");
			//create gist index
			long start = System.currentTimeMillis();
			String tablename = datasource + "_Random_" + ratio+suffix;
			String query = "CREATE INDEX "+tablename+"_Gist ON "+tablename+" USING gist(location)";
			System.out.println(query);
			st.executeUpdate(query);
			OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/gist_index_time.txt", true, datasource+"\t"+ratio+"\t"+(System.currentTimeMillis()-start)+"\n");
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}

	}
}



