package org.datasyslab.feline_implemention;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import org.datasyslab.feline_implemention.Config.Distribution;

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
	public Statement st;
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

	/**
	 * create a generic table
	 * @param tablename
	 * @param attribute_a
	 * @param attrtype_a
	 */
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

			System.out.println(query+"\n");
			st.executeUpdate(query);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	//	public void CreateFelineTable(String datasource, String suffix, int ratio)
	//	{
	//		try
	//		{
	//			String tablename = String.format("feline_%s_%s_%d", datasource, suffix, ratio);
	//			ArrayList<String> attribute_a = new ArrayList<String>(){
	//				{
	//					add("id");
	//					add("location");
	//					add("level");
	//					add("X");
	//					add("Y");
	//					add("middle");
	//					add("post");
	//				}
	//			};
	//			ArrayList<String> attrtype_a = new ArrayList<String>(){
	//				{
	//					add("bigint");
	//					add("point");
	//					add("bigint");
	//					add("bigint");
	//					add("bigint");
	//					add("bigint");
	//					add("bigint");
	//				}
	//			};
	//			CreateTable(tablename, attribute_a, attrtype_a);
	//			
	//		}
	//		catch(Exception e)
	//		{
	//			System.out.println(e.getMessage());
	//		}
	//	}

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

	public void DropIndex(String index_name)
	{
		try
		{	
			String query = String.format("drop index %s", index_name);
			System.out.println(query);
			st.executeUpdate(query);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}


	//	public static void CreateTable()
	//	{
	//		Postgres_Operation psql = new Postgres_Operation();
	//		try
	//		{
	//			int ratio = 20;
	//			for(String datasource : datasource_a)
	//			{
	//				for(String suffix : suffix_a)
	//				{
	//					psql.CreateFelineTable(datasource, suffix, ratio);
	//				}
	//			}
	//		}
	//		catch(Exception e)
	//		{
	//			System.out.println(e.getMessage());
	//		}
	//		finally
	//		{
	//			psql.DisConnect();
	//		}	
	//	}

	public static void CreateTableYelp()
	{
		Postgres_Operation psql = null;
		try
		{
			psql = new Postgres_Operation();
			String datasource = "Gowalla";
			for(int target_folder = 2; target_folder <=2; target_folder ++)
			{
				ArrayList<String> attribute_a = new ArrayList<String>(){
					{
						add("id");
						add("scc_id");
						add("location");
					}
				};
				ArrayList<String> type_a = new ArrayList<String>(){
					{
						add("bigint");
						add("bigint");
						add("point");
					}
				};

				psql.CreateTable(String.format("%s_%d", datasource, target_folder), attribute_a, type_a);


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
			int ratio = 20;
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
			int ratio = 20;
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

	public static void LoadDataYelp()
	{
		try
		{
			Postgres_Operation psql = new Postgres_Operation();

			String datasource = "Gowalla";
			int target_folder = 2;
			{
				String tablename = String.format("%s_%d", datasource, target_folder);
				String file_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/entity_psql.txt", datasource, "Random_spatial_distributed", target_folder);
				String query = String.format("copy %s from '%s' using delimiters ' ' with null as 'null'", tablename, file_path);
				OwnMethods.Print(query);
				psql.st.execute(query);

				long start = System.currentTimeMillis();
				psql.CreateGistIndex(tablename, "location");
				OwnMethods.Print(System.currentTimeMillis() - start);
			}


			psql.DisConnect();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void GetGistConstructionTime()
	{
		String tablename = "Yelp_random_80";
		String att_name = "location";
		long time = System.currentTimeMillis();

		Postgres_Operation psql = new Postgres_Operation();
		long start = System.currentTimeMillis();
		psql.CreateGistIndex(tablename, att_name);
		OwnMethods.Print(System.currentTimeMillis() - start);
		psql.DisConnect();
	}

	public static void loadDataRatio()
	{
		try {
			String distribution = Distribution.Random_spatial_distributed.name();
			Postgres_Operation psql = new Postgres_Operation();
			for(String datasource : datasource_a)
			{
				for (int ratio = 20; ratio < 90; ratio += 20)
				{
					//create table
					ArrayList<String> attribute_a = new ArrayList<String>(Arrays.asList("id", "scc_id", "location"));
					ArrayList<String> type_a = new ArrayList<String>(Arrays.asList("bigint", "bigint", "point"));
					String tablename = String.format("%s_%s_%d", datasource, distribution, ratio);
					psql.CreateTable(tablename, attribute_a, type_a);
					
					//load data
					String file_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/%s/%d/entity_psql.txt", datasource, distribution, ratio);
					String query = String.format("copy %s from '%s' using delimiters ' ' with null as 'null'", tablename, file_path);
					OwnMethods.Print(query);
					psql.st.execute(query);
					
					//create index
					psql.CreateGistIndex(tablename, "location");
				}
			}
			psql.DisConnect();
		} catch (Exception e) {
			e.printStackTrace();System.exit(-1);
		}
	}

	public static void main(String[] args) {

		//		CreateTableYelp();
		//		LoadDataYelp();

		//		GetGistConstructionTime();
		//		Yelp();
		// TODO Auto-generated method stub
		//		CreateTable();
//		LoadData();
//		CreateGistIndex();
		
		loadDataRatio();
		
	}

}
