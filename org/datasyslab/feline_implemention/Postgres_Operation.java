package org.datasyslab.feline_implemention;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Postgres_Operation {
	
	public static ArrayList<String> datasource_a = new ArrayList<String>(){{
	    add("citeseerx");
	    add("go_uniprot");
	    add("Patents");
	    add("uniprotenc_150m");
//		add("uniprotenc_22m");
//	    add("uniprotenc_100m");
	    
	}};
	
	public static ArrayList<String> distribution_a = new ArrayList<String>(){{
		add("Random_spatial_distributed");
//		add("Clustered_distributed");
//		add("Zipf_distributed");
	}};
	
	public static ArrayList<String> suffix_a = new ArrayList<String>(){{
		add("random");
//		add("clustered");
//		add("zipf");
	}};
	
	public static ArrayList<String> feline_attribute_a = new ArrayList<String>(){
		{
			add("id");
			add("location");
			add("level");
			add("X");
			add("Y");
			add("middle");
			add("post");
		}
	};
	
	public static ArrayList<String> feline_attrtype_a = new ArrayList<String>(){
		{
			add("bigint");
			add("point");
			add("bigint");
			add("bigint");
			add("bigint");
			add("bigint");
			add("bigint");
		}
	};
	
	private Connection con;
	private Statement st;
	private ResultSet rs;
	
	public Postgres_Operation()
	{
		con = PostgresJDBC.GetConnection();
		
		try {
			st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void DisConnect()
	{
		PostgresJDBC.Close(st);
		PostgresJDBC.Close(con);
	}
	
	public void CreateTable(String tablename, ArrayList<String> attribute_a, ArrayList<String> attrtype_a)
	{
		try
		{
			if(attribute_a.size() != attrtype_a.size())
			{
				OwnMethods.Print("Attributes number and type number inequal!");
				return;
			}

			if(attribute_a.size()<=0)
			{
				OwnMethods.Print("Attributes number less than 1");
				return;
			}

			String query = String.format("create table %s (", tablename);

			query += String.format("%s %s", attribute_a.get(0), attrtype_a.get(0));
			if(attribute_a.size()>1)
			{
				for(int i = 1;i<attribute_a.size();i++)
					query += String.format(",%s %s", attribute_a.get(i), attrtype_a.get(i));
			}
			query += ")";

			System.out.println(query);
			st.executeUpdate(query);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	public void CreateFelineTable(String datasource, String suffix, int ratio)
	{
		try
		{
			String tablename = String.format("feline_%s_%s_%d", datasource, suffix, ratio);
			ArrayList<String> attribute_a = new ArrayList<String>(){
				{
					add("id");
					add("location");
					add("level");
					add("X");
					add("Y");
					add("middle");
					add("post");
				}
			};
			ArrayList<String> attrtype_a = new ArrayList<String>(){
				{
					add("bigint");
					add("point");
					add("bigint");
					add("bigint");
					add("bigint");
					add("bigint");
					add("bigint");
				}
			};
			CreateTable(tablename, attribute_a, attrtype_a);
			
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	public void DropTable(String datasource, String suffix, int ratio)
	{
		try
		{
			try
			{
				String query = String.format("drop table %s_%s_%d", datasource, suffix, ratio);
				System.out.println(query);
				st.executeUpdate(query);
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	
	public void CreateGistIndex(String tablename, String attr_name)
	{
		try
		{	
			String query = String.format("create index %s_%s_gist on %s using gist(%s)", tablename, attr_name, tablename, attr_name);
			System.out.println(query);
			st.executeUpdate(query);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}

	}
	
	
	public static void CreateTable()
	{
		Postgres_Operation psql = new Postgres_Operation();
		try
		{
			int ratio = 80;
			for(String datasource : datasource_a)
			{
				for(String suffix : suffix_a)
				{
					psql.CreateFelineTable(datasource, suffix, ratio);
				}
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			psql.DisConnect();
		}	
	}
	
	public static void LoadData()
	{
		Postgres_Operation psql = null;
		try
		{
			psql = new Postgres_Operation();
			int ratio = 80;
			for(String datasource : datasource_a)
			{
				for(int i = 0;i<suffix_a.size();i++)
				{
					String suffix = suffix_a.get(i);
					String distribution = distribution_a.get(i);
					String tablename = String.format("feline_%s_%s_%d", datasource, suffix, ratio);
					String file_path = String.format("/home/yuhansun/Documents/Real_data/%s/%s/%d/entity_feline.txt", datasource, distribution, ratio);
					String query = String.format("copy %s from '%s' using delimiters ' ' with null as 'null'", tablename, file_path);
					OwnMethods.Print(query);
					psql.st.execute(query);
				}
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			psql.DisConnect();
		}	
	}
	
	public static void CreateGistIndex()
	{
		Postgres_Operation psql = null;
		try
		{
			psql = new Postgres_Operation();
			int ratio = 80;
			for(String datasource : datasource_a)
			{
				for(int i = 0;i<suffix_a.size();i++)
				{
					String suffix = suffix_a.get(i);
					String tablename = String.format("feline_%s_%s_%d", datasource, suffix, ratio);
					psql.CreateGistIndex(tablename, "location");
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			psql.DisConnect();
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//CreateTable();
		//LoadData();
		CreateGistIndex();
	}

}
