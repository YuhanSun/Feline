package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.management.Query;

import org.datasyslab.feline_implemention.Config.Distribution;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.impl.util.Access;
import org.neo4j.register.Register.Int;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.AsyncViewResource.Builder;
import com.sun.jersey.spi.StringReader;

public class SpaReachPLL{

	public static ArrayList<Integer> ReadConvertTable(String filepath)
	{
		ArrayList<Integer> table = new ArrayList<Integer>();
		BufferedReader reader = null;
		String string = null;
		try
		{
			reader = new BufferedReader(new FileReader(new File(filepath)));
			while((string = reader.readLine())!=null)
			{
				String[] l_Strings = string.split("\t");
				int scc_id = Integer.parseInt(l_Strings[1]);
				table.add(scc_id);
			}
			reader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return table;
	}




	public static ArrayList<MyRectangle> ReadExperimentQueryRectangle(String filepath)
	{
		ArrayList<MyRectangle> queryrectangles = new ArrayList<MyRectangle>();
		BufferedReader reader  = null;
		File file = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null)
			{
				String[] line_list = temp.split("\t");
				MyRectangle rect = new MyRectangle(Double.parseDouble(line_list[0]), Double.parseDouble(line_list[1]), Double.parseDouble(line_list[2]), Double.parseDouble(line_list[3]));
				queryrectangles.add(rect);
			}
			reader.close();
		}
		catch(Exception e)
		{

			e.printStackTrace();
		}
		finally
		{
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
		}
		return queryrectangles;
	}

	public static String password = "syh19910205";

	public PostgresJDBC postgresJDBC;
	private String tablename;
	public String location_columnname = "location";

	public Neo4j_Graph_Store neo4j_Graph_Store;

	public long postgresql_time;
	public long neo4j_time;
	public long judge_time;

	public int AccessNodeCount = 0;
	public int Neo4jAccessCount = 0;
	public int locate_count = 0;

	public SpaReachPLL(String tablename)
	{
		this.tablename = tablename;
		postgresJDBC = new PostgresJDBC();	
		neo4j_Graph_Store = new Neo4j_Graph_Store();
		postgresql_time = 0;
		neo4j_time = 0;
		judge_time = 0;
	}

	public void Disconnect()
	{
		postgresJDBC.DisConnect();
	}

	public static void LoadData(String from_filepath, String to_filepath, String table_filepath, String db_path)
	{
		OwnMethods.Print(String.format("Loading data into %s", db_path));
		BatchInserter inserter = null;
		BufferedReader reader = null;
		BufferedReader reader_reachFrom = null;
		BufferedReader reader_reachTo = null;
		File file = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "5g");

		try
		{
			Map<Integer,Integer> table = new HashMap<Integer, Integer>();
			file = new File(table_filepath);
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine())!=null)
			{		
				String[] l = tempString.split("\t");
				int first = Integer.parseInt(l[0]);
				int second = Integer.parseInt(l[1]);
				table.put(second, first);
			}
			reader.close();

			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
			File file_reachFrom = new File(from_filepath);
			File file_reachTo = new File(to_filepath);
			reader_reachFrom = new BufferedReader(new FileReader(file_reachFrom));
			reader_reachTo = new BufferedReader(new FileReader(file_reachTo));
			String str_reachFrom = null, str_reachTo = null;
			Label Reach_Index_Label = DynamicLabel.label("Reachability_Index");
			while(((str_reachFrom = reader_reachFrom.readLine())!=null)&&((str_reachTo = reader_reachTo.readLine())!=null))
			{
				String[] l_rF = str_reachFrom.split("\t");
				String[] l_rT = str_reachTo.split("\t");
				int scc_id = Integer.parseInt(l_rF[0]);
				int id = table.get(scc_id);
				Map<String, Object> properties = new HashMap<String, Object>();
				properties.put("scc_id", scc_id);
				properties.put("id", id);

				if(l_rF.length>1)
				{
					int[] l = new int[l_rF.length-1];
					for(int i = 0;i<l.length;i++)
						l[i] = Integer.parseInt(l_rF[i+1]);
					properties.put("reachFrom", l);
				}
				if(l_rT.length>1)
				{
					int[] l = new int[l_rT.length-1];
					for(int i = 0;i<l.length;i++)
						l[i] = Integer.parseInt(l_rT[i+1]);
					properties.put("reachTo", l);
				}
				inserter.createNode(scc_id, properties, Reach_Index_Label);
			}
		}
		catch(IOException e)
		{
			if(inserter!=null)
				inserter.shutdown();
			e.printStackTrace();
		}
		finally
		{
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
			if(reader_reachFrom!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
			if(reader_reachTo!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
		}
	}

	public static void LoadData()
	{
		//		String datasource = "Patents";
//		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
//		//		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_150m");

		//		for(String datasource: datasource_a)
		String datasource = "Patents";
		{
			String from_filepath = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/reachFromIndex.txt", datasource);
			String to_filepath = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/reachToIndex.txt", datasource);
			String table_filepath = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/table.txt", datasource);
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL/data/graph.db", datasource);
			LoadData(from_filepath, to_filepath, table_filepath, db_path);
		}

	}

	public static void LoadDataYelp()
	{
		String datasource = "Gowalla";
		String distribution = "Random_spatial_distributed";
		int target_folder = 2;
		{
			String from_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/reachFromIndex.txt", datasource, distribution, target_folder);
			String to_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/reachToIndex.txt", datasource, distribution, target_folder);
			String table_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/table.txt", datasource, distribution, target_folder);
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL_%d/data/graph.db", datasource, target_folder);
			LoadData(from_filepath, to_filepath, table_filepath, db_path);
		}

	}

	public ResultSet RangeQuery(MyRectangle rect)
	{
		String query = String.format("select distinct scc_id from %s where %s <@ box '((%f,%f),(%f,%f))'", tablename, location_columnname, rect.min_x, rect.min_y, rect.max_x, rect.max_y);
		ResultSet resultSet = postgresJDBC.Execute(query);
		//		OwnMethods.Print(query);
		return resultSet;
	}

	public boolean ReachabilityQuery(int start_id, MyRectangle rect, ArrayList<Integer> converTable) 
	{
		String query = "";
		ResultSet resultSet = null;
		try
		{
			postgresql_time = 0;
			neo4j_time = 0;
			AccessNodeCount = 0;
			locate_count = 0;

			start_id = converTable.get(start_id);

			long start = System.currentTimeMillis();
			resultSet = this.RangeQuery(rect);
			postgresql_time+=System.currentTimeMillis() - start;
			if(!resultSet.next())
				return false;
			else
				resultSet.previous();

			query = "match (n:Reachability_Index) where id(n) = " + start_id + " return n";
			AccessNodeCount+=1;
			Neo4jAccessCount+=1;

			start = System.currentTimeMillis();
			String result = neo4j_Graph_Store.Execute(query);
			JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
			neo4j_time += System.currentTimeMillis() - start;

			JsonObject jsonOb = jsonArr.get(0).getAsJsonObject();
			jsonArr = jsonOb.get("row").getAsJsonArray();
			jsonOb = jsonArr.get(0).getAsJsonObject();
			int source_scc_id = jsonOb.get("scc_id").getAsInt();

			Type listType = new TypeToken<ArrayList<Integer>>() {}.getType();
			ArrayList<Integer> source_reachTo = new Gson().fromJson(jsonOb.get("reachTo"), listType);

			int bulksize = 1;
			int i = 0;
			ArrayList<Integer> trg_l = new ArrayList<Integer>();
			while(resultSet.next())
			{	
				locate_count++;
				if(i != bulksize)
				{
					int target_id = converTable.get(resultSet.getInt("scc_id"));
					if(start_id == target_id)
						return true;
					trg_l.add(target_id);
					i++;
					if(i!=bulksize)
						continue;
				}

				if(i == bulksize);
				{
					query = String.format("match (n) where id(n) in %s return n", trg_l.toString());

					start = System.currentTimeMillis();
					result = neo4j_Graph_Store.Execute(query);
					jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
					neo4j_time+=System.currentTimeMillis() - start;

					AccessNodeCount+=bulksize;
					Neo4jAccessCount+=1;

					start = System.currentTimeMillis();
					for(int j = 0;j<jsonArr.size();j++)
					{
						jsonOb = jsonArr.get(j).getAsJsonObject();
						JsonArray row = jsonOb.get("row").getAsJsonArray();
						jsonOb = row.get(0).getAsJsonObject();
						int target_scc_id = jsonOb.get("scc_id").getAsInt();
						if(source_scc_id >= target_scc_id)
							continue;
						else
						{
							ArrayList<Integer> target_reachFrom = new Gson().fromJson(jsonOb.get("reachFrom"), listType);
							int sn = source_reachTo.size(), tn = target_reachFrom.size();
							int si = 0, ti = 0;

							while(si < sn && ti < tn) 
							{
								int sp = source_reachTo.get(si), tp = target_reachFrom.get(ti);
								if (sp == tp) {
									//						        	System.out.println(source_scc_id);
									//						        	System.out.println(target_scc_id);
									//						        	System.out.println(source_reachTo.get(si));
									judge_time += System.currentTimeMillis() - start;
									return true;
								}
								if (sp <= tp) {
									si++;
								} else {
									ti++;
								}
							}
							continue;
						}
					}
					judge_time += System.currentTimeMillis() - start;

					i = 0;
					trg_l.clear();
				}
			}

			if(i!=0)
			{
				query=String.format("match (n) where id(n) in %s return n", trg_l);
				Neo4jAccessCount+=1;
				AccessNodeCount += i;

				start = System.currentTimeMillis();
				result = neo4j_Graph_Store.Execute(query);
				jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
				neo4j_time += System.currentTimeMillis() - start;

				start = System.currentTimeMillis();
				for(int j = 0;j<jsonArr.size();j++)
				{
					jsonOb = jsonArr.get(j).getAsJsonObject();
					JsonArray row = jsonOb.get("row").getAsJsonArray();
					jsonOb = row.get(0).getAsJsonObject();
					int target_scc_id = jsonOb.get("scc_id").getAsInt();
					if(source_scc_id >= target_scc_id)
						continue;
					else
					{
						ArrayList<Integer> target_reachFrom = new Gson().fromJson(jsonOb.get("reachFrom"), listType);
						int sn = source_reachTo.size(), tn = target_reachFrom.size();
						int si = 0, ti = 0;

						while(si < sn && ti < tn) 
						{
							int sp = source_reachTo.get(si), tp = target_reachFrom.get(ti);
							if (sp == tp) {
								//					        	System.out.println(source_scc_id);
								//					        	System.out.println(target_scc_id);
								//					        	System.out.println(source_reachTo.get(si));
								judge_time += System.currentTimeMillis() - start;
								return true;
							}
							if (sp <= tp) {
								si++;
							} else {
								ti++;
							}
						}
						continue;
					}
				}
				judge_time += System.currentTimeMillis() - start;
			}
			//			while(rs.next())
			//			{
			//				int target_id = Integer.parseInt(rs.getObject("scc_id").toString());
			//				query = "match (n:Reachability_Index) where n.id = " + target_id + " return n";
			//				result = p_neo.Execute(query);
			//				jsonArr = p_neo.GetExecuteResultDataASJsonArray(result);
			//				jsonOb = jsonArr.get(0).getAsJsonObject();
			//				jsonArr = jsonOb.get("row").getAsJsonArray();
			//				jsonOb = jsonArr.get(0).getAsJsonObject();
			//				int target_scc_id = jsonOb.get("scc_id").getAsInt();
			//				if(source_scc_id > target_scc_id)
			//					continue;
			//				else
			//				{
			//					ArrayList<Integer> target_reachFrom = new Gson().fromJson(jsonOb.get("reachFrom"), listType);
			//					int sn = source_reachTo.size(), tn = target_reachFrom.size();
			//				    int si = 0, ti = 0;
			//				    
			//				    while(si < sn && ti < tn) {
			//				        int sp = source_reachTo.get(si), tp = target_reachFrom.get(ti);
			//				        if (sp == tp) {
			//				        	System.out.println(source_scc_id);
			//				        	System.out.println(target_scc_id);
			//				        	System.out.println(source_reachTo.get(si));
			//				            return true;
			//				        }
			//				        if (sp <= tp) {
			//				            si++;
			//				        } else {
			//				            ti++;
			//				        }
			//				    }
			//				    continue;
			//				}
			//			}						
			return false;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			OwnMethods.Print(query);
			System.exit(-1);
			return false;
		}
		finally {
			PostgresJDBC.Close(resultSet);
		}
	}

	public static void Experiment()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		//		datasource_a.add("citeseerx");
		//				datasource_a.add("go_uniprot");
		//				datasource_a.add("Patents");
		//				datasource_a.add("uniprotenc_150m");

		String suffix = "random";

		//		for(int name_index = 0;name_index<datasource_a.size();name_index++)
		{
			String datasource = "Yelp";
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_PLL_"+suffix+".csv";
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");

			String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/SCC.txt", datasource);
			String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/graph_entity.txt", datasource);
			ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);

			String convertTable_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/table.txt", datasource);
			ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
			{
				//				for(int ratio = 20;ratio<=20;ratio+=20)
				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tPLL_time\tvisit_node_count\tlocated_in_count\ttrue_count\n");

					ArrayList<String> tmp_al = OwnMethods.ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Integer absolute_id = Integer.parseInt(tmp_al.get(i));
						String x = absolute_id.toString();
						experiment_id_al.add(x);
					}

					double selectivity = 0.000001;
					boolean isrun = true;
					boolean isbreak = false;
					int experiment_count = 20;
					//					int experiment_count = 5;
					{
						while(selectivity<=0.11)
						{
							OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

							int log = (int)Math.log10(selectivity);
							String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
							ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);


							int true_count = 0;
							//PLL
							OwnMethods.Print(PostgresJDBC.StopServer(password));
							OwnMethods.Print(OwnMethods.ClearCache(password));
							OwnMethods.Print(PostgresJDBC.StartServer(password));
							OwnMethods.Print(Neo4j_Graph_Store.StartServer(db_path));

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							String table_name = String.format("%s_random_%d",datasource, ratio);
							SpaReachPLL PLL= new SpaReachPLL(table_name);

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							int accessnodecount = 0;
							int time_PLL = 0, time_spa = 0, time_reach = 0, locate_count = 0;
							for(int i = 0;i<experiment_count;i++)
							{

								MyRectangle query_rect = queryrectangles.get(i);

								OwnMethods.Print(String.format("index:%d\t", i));
								int id = Integer.parseInt(experiment_id_al.get(i));
								OwnMethods.Print(String.format("ori_id:\t%d", id));

								try
								{
									long start = System.currentTimeMillis();
									id = refer_table.get(id);
									boolean result3 = PLL.ReachabilityQuery(id, query_rect, converTable);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", PLL.postgresql_time, PLL.neo4j_time));
									OwnMethods.Print(String.format("Time:%d", time));
									OwnMethods.Print(String.format("Locate count:%d\n", PLL.locate_count));
									if(result3)
										true_count++;

									time_PLL += time;
									time_spa += PLL.postgresql_time;
									time_reach += PLL.neo4j_time;
									accessnodecount += PLL.AccessNodeCount;
									locate_count += PLL.locate_count;

								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.Print(String.format("\nTotal locate count:\t%s\n", locate_count));
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_PLL/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + locate_count/experiment_count + "\t");
							if(time_PLL/experiment_count >= 10000 && experiment_count == 500)
								experiment_count =3;

							PLL.Disconnect();
							OwnMethods.Print(Neo4j_Graph_Store.StopServer(db_path));

							if(isbreak)
								break;
							OwnMethods.WriteFile(resultpath, true, true_count+"\n");
							selectivity*=10;
						}
					}
				}
				OwnMethods.WriteFile(resultpath, true, "\n");
			}
			OwnMethods.WriteFile(resultpath, true, "\n");
		}		
	}

	public static void ExperimentColdPostgres()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		//		datasource_a.add("citeseerx");
		//				datasource_a.add("go_uniprot");
		//				datasource_a.add("Patents");
		//				datasource_a.add("uniprotenc_150m");

		String suffix = "random";

		//		for(int name_index = 0;name_index<datasource_a.size();name_index++)
		{
			String datasource = "Yelp";
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_PLL_"+suffix+".csv";
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
			OwnMethods.WriteFile(resultpath, true, datasource+"(ColdPostgres)\n");

			String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/SCC.txt", datasource);
			String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/graph_entity.txt", datasource);
			ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);

			String convertTable_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/table.txt", datasource);
			ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
			{
				//				for(int ratio = 20;ratio<=20;ratio+=20)
				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tPLL_time\tvisit_node_count\tlocated_in_count\ttrue_count\n");

					ArrayList<String> tmp_al = OwnMethods.ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Integer absolute_id = Integer.parseInt(tmp_al.get(i));
						String x = absolute_id.toString();
						experiment_id_al.add(x);
					}

					double selectivity = 0.000001;
					boolean isrun = true;
					boolean isbreak = false;
					int experiment_count = 10;
					//					int experiment_count = 5;
					{
						while(selectivity<=0.11)
						{
							OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

							int log = (int)Math.log10(selectivity);
							String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
							ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);


							int true_count = 0;
							//PLL
							OwnMethods.Print(OwnMethods.ClearCache(password));
							OwnMethods.Print(Neo4j_Graph_Store.StartServer(db_path));

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							String table_name = String.format("%s_random_%d",datasource, ratio);


							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							int accessnodecount = 0;
							int time_PLL = 0, time_spa = 0, time_reach = 0, locate_count = 0;
							for(int i = 0;i<experiment_count;i++)
							{

								MyRectangle query_rect = queryrectangles.get(i);

								OwnMethods.Print(String.format("index:%d\t", i));
								int id = Integer.parseInt(experiment_id_al.get(i));
								OwnMethods.Print(String.format("ori_id:\t%d", id));

								try
								{

									//									OwnMethods.Print(PostgresJDBC.StopServer(password));
									//									OwnMethods.Print(OwnMethods.ClearCache(password));
									//									OwnMethods.Print(PostgresJDBC.StartServer(password));

									PostgresJDBC.StopServer(password);
									OwnMethods.ClearCache(password);
									PostgresJDBC.StartServer(password);

									SpaReachPLL PLL= new SpaReachPLL(table_name);

									long start = System.currentTimeMillis();
									id = refer_table.get(id);
									boolean result3 = PLL.ReachabilityQuery(id, query_rect, converTable);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", PLL.postgresql_time, PLL.neo4j_time));
									OwnMethods.Print(String.format("Total Time:%d", time));
									if(result3)
										true_count++;

									time_PLL += time;
									time_spa += PLL.postgresql_time;
									time_reach += PLL.neo4j_time;
									accessnodecount += PLL.AccessNodeCount;
									locate_count += PLL.locate_count;
									OwnMethods.Print(String.format("Locate count:%d\n", PLL.locate_count));
									PLL.Disconnect();

								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.Print(String.format("\nTotal locate count:\t%s\n", locate_count));
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_PLL/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + locate_count/experiment_count + "\t");
							if(time_PLL/experiment_count >= 10000 && experiment_count == 500)
								experiment_count =3;

							OwnMethods.Print(Neo4j_Graph_Store.StopServer(db_path));

							if(isbreak)
								break;
							OwnMethods.WriteFile(resultpath, true, true_count+"\n");
							selectivity*=10;
						}
					}
				}
				OwnMethods.WriteFile(resultpath, true, "\n");
			}
			OwnMethods.WriteFile(resultpath, true, "\n");
		}		
	}

	public static void ExperimentColdPostgresNeo4j_backup()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		//		datasource_a.add("citeseerx");
		//				datasource_a.add("go_uniprot");
		//				datasource_a.add("Patents");
		//				datasource_a.add("uniprotenc_150m");

		String suffix = "random";

		//		for(int name_index = 0;name_index<datasource_a.size();name_index++)
		{
			String datasource = "Yelp";
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_PLL_"+suffix+".csv";
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
			OwnMethods.WriteFile(resultpath, true, datasource+"(ColdPostgresNeo4j)\n");

			String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/SCC.txt", datasource);
			String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/graph_entity.txt", datasource);
			ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);

			String convertTable_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/table.txt", datasource);
			ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
			{
				//				for(int ratio = 20;ratio<=20;ratio+=20)
				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tPLL_time\tvisit_node_count\tlocated_in_count\ttrue_count\n");

					ArrayList<String> tmp_al = OwnMethods.ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Integer absolute_id = Integer.parseInt(tmp_al.get(i));
						String x = absolute_id.toString();
						experiment_id_al.add(x);
					}

					double selectivity = 0.000001;
					boolean isrun = true;
					boolean isbreak = false;
					int experiment_count = 100;
					//					int experiment_count = 5;
					{
						while(selectivity<=0.11)
						{
							OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

							int log = (int)Math.log10(selectivity);
							String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
							ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);

							int true_count = 0;
							//PLL
							String table_name = String.format("%s_random_%d",datasource, ratio);

							int accessnodecount = 0;
							int time_PLL = 0, time_spa = 0, time_reach = 0, locate_count = 0;
							for(int i = 0;i<experiment_count;i++)
							{

								MyRectangle query_rect = queryrectangles.get(i);

								OwnMethods.Print(String.format("index:%d\t", i));
								int id = Integer.parseInt(experiment_id_al.get(i));
								OwnMethods.Print(String.format("ori_id:\t%d", id));

								try
								{

									//									OwnMethods.Print(PostgresJDBC.StopServer(password));
									//									OwnMethods.Print(OwnMethods.ClearCache(password));
									//									OwnMethods.Print(PostgresJDBC.StartServer(password));


									PostgresJDBC.StopServer(password);
									OwnMethods.ClearCache(password);
									PostgresJDBC.StartServer(password);
									Neo4j_Graph_Store.StartServer(db_path);

									try {
										Thread.currentThread().sleep(3000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}

									SpaReachPLL PLL= new SpaReachPLL(table_name);

									long start = System.currentTimeMillis();
									id = refer_table.get(id);
									boolean result3 = PLL.ReachabilityQuery(id, query_rect, converTable);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", PLL.postgresql_time, PLL.neo4j_time));
									OwnMethods.Print(String.format("Total Time:%d", time));
									if(result3)
										true_count++;

									time_PLL += time;
									time_spa += PLL.postgresql_time;
									time_reach += PLL.neo4j_time;
									accessnodecount += PLL.AccessNodeCount;
									locate_count += PLL.locate_count;
									OwnMethods.Print(String.format("Locate count:%d\n", PLL.locate_count));
									PLL.Disconnect();
									OwnMethods.Print(Neo4j_Graph_Store.StopServer(db_path));

								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.Print(String.format("\nTotal locate count:\t%s\n", locate_count));
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_PLL/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + locate_count/experiment_count + "\t");
							if(time_PLL/experiment_count >= 10000 && experiment_count == 500)
								experiment_count =3;

							if(isbreak)
								break;
							OwnMethods.WriteFile(resultpath, true, true_count+"\n");
							selectivity*=10;
						}
					}
				}
				OwnMethods.WriteFile(resultpath, true, "\n");
			}
			OwnMethods.WriteFile(resultpath, true, "\n");
		}		
	}

	public static void ExperimentColdPostgresNeo4j(int target_folder)
	{
		String datasource = "Gowalla";
		String distribution = "Random_spatial_distributed";
		//		int target_folder = 2;
		String resultpath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/result/%s_PLL_querytime.csv", datasource);
		String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL_%d", datasource, target_folder);
		OwnMethods.WriteFile(resultpath, true, datasource + "\t"+ target_folder +"\t(ColdPostgresNeo4j)\n");

		String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/SCC.txt", datasource, distribution, target_folder);
		String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/graph_entity_newformat.txt", datasource, distribution, target_folder);
		ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);

		String convertTable_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/table.txt", datasource, distribution, target_folder);
		ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
		{
			{
				OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tPLL_time\tvisit_node_count\tlocated_in_count\ttrue_count\n");

				String querynodeid_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/experiment_id.txt", datasource, distribution, target_folder);
				ArrayList<String> experiment_id_al = OwnMethods.ReadExperimentNodeGeneral(querynodeid_filepath);

				double selectivity = 0.0001;
				//				double selectivity = 0.0001;
				boolean isrun = true;
				boolean isbreak = false;
				int experiment_count = 20;
				//					int experiment_count = 5;
				{
					while(selectivity<=0.11)
					{
						OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

						int log = (int)Math.log10(selectivity);
						String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
						ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);

						int true_count = 0;
						//PLL
						String table_name = String.format("%s_%d",datasource, target_folder);

						int accessnodecount = 0;
						int time_PLL = 0, time_spa = 0, time_reach = 0, locate_count = 0;
						for(int i = 0;i<experiment_count;i++)
						{
							MyRectangle query_rect = queryrectangles.get(i);

							OwnMethods.Print(String.format("index:%d\t", i));
							int id = Integer.parseInt(experiment_id_al.get(i));
							OwnMethods.Print(String.format("ori_id:\t%d", id));

							try
							{
								PostgresJDBC.StopServer(password);
								OwnMethods.ClearCache(password);
								PostgresJDBC.StartServer(password);
								OwnMethods.Print(Neo4j_Graph_Store.StartServer(db_path));;

								try {
									Thread.currentThread().sleep(2000);
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}

								SpaReachPLL PLL= new SpaReachPLL(table_name);

								long start = System.currentTimeMillis();
								id = refer_table.get(id);
								OwnMethods.Print(String.format("da_id:\t%d", id));

								boolean result3 = PLL.ReachabilityQuery(id, query_rect, converTable);
								long time = System.currentTimeMillis() - start;
								OwnMethods.Print(result3);
								OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", PLL.postgresql_time, PLL.neo4j_time));
								OwnMethods.Print(String.format("Total Time:%d", time));
								if(result3)
									true_count++;

								time_PLL += time;
								time_spa += PLL.postgresql_time;
								time_reach += PLL.neo4j_time;
								accessnodecount += PLL.AccessNodeCount;
								locate_count += PLL.locate_count;
								OwnMethods.Print(String.format("Locate count:%d\n", PLL.locate_count));
								PLL.Disconnect();
								OwnMethods.Print(Neo4j_Graph_Store.StopServer(db_path));

							}
							catch(Exception e)
							{
								e.printStackTrace();
								OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, LocalDate.now().toString() + "\t" +e.getMessage().toString()+"\n");
								i = i-1;
							}						
						}
						OwnMethods.Print(String.format("\nTotal locate count:\t%s\n", locate_count));
						OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_PLL/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + locate_count/experiment_count + "\t");
						//						if(time_PLL / experiment_count >= 50000 && experiment_count >=10)
						//							experiment_count = 10;
						//						if(time_PLL/experiment_count >= 100000 && experiment_count >= 5)
						//							experiment_count =3;

						if(isbreak)
							break;
						OwnMethods.WriteFile(resultpath, true, true_count+"\n");
						selectivity*=10;
					}
				}
			}
			OwnMethods.WriteFile(resultpath, true, "\n");
		}
		OwnMethods.WriteFile(resultpath, true, "\n");
	}

	public static void ExperimentCorrectness()
	{
		String datasource = "Yelp";
		String distribution = "Random_spatial_distributed";
		int target_folder = 2;
		String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL_%d", datasource, target_folder);

		String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/SCC.txt", datasource, distribution, target_folder);
		String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/graph_entity_newformat.txt", datasource, distribution, target_folder);
		ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);

		String convertTable_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/table.txt", datasource, distribution, target_folder);
		ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
		{
			{

				String querynodeid_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/experiment_id.txt", datasource, distribution, target_folder);
				ArrayList<String> experiment_id_al = OwnMethods.ReadExperimentNodeGeneral(querynodeid_filepath);

				double selectivity = 0.00001;
				boolean isrun = true;
				boolean isbreak = false;
				int experiment_count = 20;
				//					int experiment_count = 5;
				{
					//					while(selectivity<=0.11)
					{

						int log = (int)Math.log10(selectivity);
						String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
						ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);

						int true_count = 0;
						//PLL
						String table_name = String.format("%s_%d",datasource, target_folder);
						SpaReachPLL PLL= new SpaReachPLL(table_name);

						int accessnodecount = 0;
						int time_PLL = 0, time_spa = 0, time_reach = 0, locate_count = 0;
						for(int i = 0;i<experiment_count;i++)
						{
							MyRectangle query_rect = queryrectangles.get(i);

							OwnMethods.Print(String.format("index:%d\t", i));
							int id = Integer.parseInt(experiment_id_al.get(i));
							OwnMethods.Print(String.format("ori_id:\t%d", id));

							try
							{
								long start = System.currentTimeMillis();
								id = refer_table.get(id);
								OwnMethods.Print(String.format("dag_id:\t%d", id));

								boolean result3 = PLL.ReachabilityQuery(id, query_rect, converTable);
								long time = System.currentTimeMillis() - start;
								OwnMethods.Print(result3);
								OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", PLL.postgresql_time, PLL.neo4j_time));
								OwnMethods.Print(String.format("Total Time:%d", time));
								if(result3)
									true_count++;

								OwnMethods.WriteFile("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/result/PLL_result.txt", true, result3 + "\n");
								time_PLL += time;
								time_spa += PLL.postgresql_time;
								time_reach += PLL.neo4j_time;
								accessnodecount += PLL.AccessNodeCount;
								locate_count += PLL.locate_count;
								OwnMethods.Print(String.format("Locate count:%d\n", PLL.locate_count));

							}
							catch(Exception e)
							{
								e.printStackTrace();
								OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, LocalDate.now().toString() + "\t" +e.getMessage().toString()+"\n");
								i = i-1;
							}						
						}
						OwnMethods.Print(String.format("\nTotal locate count:\t%s\n", locate_count));
						PLL.Disconnect();
					}
				}
			}
		}
	}

	public static void Experiment_Ratio_Hot()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_150m");

		String suffix = "random";

		for(String datasource : datasource_a)
		{
			String resultpath = "/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/result/Experiment_1/query_time_PLL_ratio_"+suffix+".csv";
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
			OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tPLL_time\tvisit_node_count\ttrue_count\n");

			String convertTable_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/table.txt", datasource);
			ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
			{
				for(int ratio = 20;ratio<=80;ratio+=20)
					//				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");

					ArrayList<String> experiment_id_al = OwnMethods.ReadExperimentNode(datasource);

					ArrayList<Double> a_x = new ArrayList<Double>();
					ArrayList<Double> a_y = new ArrayList<Double>();

					Random r = new Random();

					double selectivity = 0.0001;
					double spatial_total_range = 1000;
					boolean isrun = true;
					boolean isbreak = false;
					//					int experiment_count = experiment_id_al.size();
					int experiment_count = 10;
					{
						//						while(selectivity<=0.11)
						{
							double rect_size = spatial_total_range * Math.sqrt(selectivity);
							OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

							a_x.clear();
							a_y.clear();
							for(int i = 0;i<experiment_id_al.size();i++)
							{
								a_x.add(r.nextDouble()*(1000-rect_size));
								a_y.add(r.nextDouble()*(1000-rect_size));
							}

							int true_count = 0;
							//PLL
							OwnMethods.Print(PostgresJDBC.StopServer(password));
							OwnMethods.Print(OwnMethods.ClearCache(password));
							OwnMethods.Print(PostgresJDBC.StartServer(password));
							OwnMethods.Print(Neo4j_Graph_Store.StartServer(db_path));

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							String table_name = String.format("%s_random_%d",datasource, ratio);
							SpaReachPLL PLL= new SpaReachPLL(table_name);

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							int accessnodecount = 0;
							int time_PLL = 0, time_spa = 0, time_reach = 0;
							for(int i = 0;i<experiment_count;i++)
							{
								double x = a_x.get(i);
								double y = a_y.get(i);
								MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);

								System.out.println(i);
								int id = Integer.parseInt(experiment_id_al.get(i));
								System.out.println(id);

								try
								{
									long start = System.currentTimeMillis();
									boolean result3 = PLL.ReachabilityQuery(id, query_rect, converTable);
									long time = System.currentTimeMillis() - start;
									System.out.println(result3);
									OwnMethods.Print(String.format("Time:%d", time));
									if(result3)
										true_count++;

									time_PLL += time;
									time_reach += PLL.postgresql_time;
									time_spa += PLL.neo4j_time;
									accessnodecount += PLL.AccessNodeCount;

								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_PLL/experiment_count+"\t" + accessnodecount/experiment_count + "\t");

							PLL.Disconnect();
							System.out.println(Neo4j_Graph_Store.StopServer(db_path));

							if(isbreak)
								break;
							OwnMethods.WriteFile(resultpath, true, true_count+"\n");
						}
					}
				}
				OwnMethods.WriteFile(resultpath, true, "\n");
			}

			OwnMethods.WriteFile(resultpath, true, "\n");

		}		
	}

	public static void Experiment_Ratio_Cold()
	{
		try
		{
			double selectivity = 0.001;
			int experiment_count = 3;
			String distribution = Distribution.Random_spatial_distributed.name();
//			ArrayList<String> datasource_a = new ArrayList<String>(Arrays.asList("uniprotenc_150m", "Patents", "go_uniprot", "citeseerx"));
			ArrayList<String> datasource_a = new ArrayList<String>(Arrays.asList("Patents"));

			for(String datasource : datasource_a)
			{
				String graph_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/new_graph.txt", datasource);
				int nodeCount = OwnMethods.GetNodeCountGeneral(graph_path);

				//			String resultpath = "/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/result/Experiment_1/query_time_PLL_ratio_"+suffix+".csv";
				String resultpath = "/mnt/hgfs/Experiment_Result/GeoReach_Experiment/result/ratio/query_time_PLL.csv";
				String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
				OwnMethods.WriteFile(resultpath, true, datasource+ "\t" + selectivity + "\tColdPostgresNeo4j\n");
				OwnMethods.WriteFile(resultpath, true, "ratio\tSpa_time\treach_time\tPLL_time\tvisit_node_count\ttrue_count\n");

				String convertTable_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/table.txt", datasource);
				ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
				for(int ratio = 20;ratio<=80;ratio+=20)
				{
					String querynodeid_filepath = String.format("/mnt/hgfs/Experiment_Result/GeoReach_Experiment"
							+ "/experiment_query/%s/experiment_id.txt", datasource);
					ArrayList<Integer> nodeids = OwnMethods.readIntegerArray(querynodeid_filepath);

					int spaNodeCount = (int) (nodeCount * (100-ratio) / 100.0); 
					String queryrectangle_filepath = String.format("/mnt/hgfs/Experiment_Result/GeoReach_Experiment"
							+ "/experiment_query/%s/%s_%d_queryrect_%d.txt", datasource, distribution, ratio, (int) (spaNodeCount * selectivity));
					ArrayList<MyRectangle> queryrectangles = OwnMethods.ReadExperimentQueryRectangle(queryrectangle_filepath);

					int true_count = 0;

					int accessnodecount = 0;
					int time_PLL = 0, time_spa = 0, time_reach = 0;
					for(int i = 0;i<experiment_count;i++)
					{
						OwnMethods.Print(PostgresJDBC.StopServer(password));
						OwnMethods.Print(OwnMethods.ClearCache(password));
						OwnMethods.Print(PostgresJDBC.StartServer(password));
						OwnMethods.Print(Neo4j_Graph_Store.StartServer(db_path));
						
						Thread.currentThread().sleep(5000);
						
						String table_name = String.format("%s_%s_%d",datasource, distribution, ratio);
						SpaReachPLL PLL= new SpaReachPLL(table_name);
						
						Thread.currentThread().sleep(5000);
						
						System.out.println(i);
						int id = nodeids.get(i);
						MyRectangle queryrect = queryrectangles.get(i);
						
						long start = System.currentTimeMillis();
						boolean result3 = PLL.ReachabilityQuery(id, queryrect, converTable);
						long time = System.currentTimeMillis() - start;
						System.out.println(result3);
						OwnMethods.Print(String.format("Time:%d", time));
						if(result3)
							true_count++;

						time_PLL += time;
						time_spa += PLL.postgresql_time;
						time_reach += PLL.neo4j_time;
						accessnodecount += PLL.AccessNodeCount;
						
						PLL.Disconnect();
						System.out.println(Neo4j_Graph_Store.StopServer(db_path));
					}
					OwnMethods.WriteFile(resultpath, true, ratio + "\t" + time_spa/experiment_count + "\t" + time_reach/experiment_count 
							+ "\t" +time_PLL/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + true_count + "\t\n");
				}
				OwnMethods.WriteFile(resultpath, true, "\n");
			}		
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();System.exit(-1);
		}
	}
	
	/**
	 * each query is separated
	 */
	public static void Experiment_Distribution_Cold()
	{
		try 
		{
			double selectivity = 0.001;
			int experiment_count = 3;
			int ratio = 20;
			ArrayList<String> datasource_a = new ArrayList<String>(Arrays.asList("uniprotenc_150m", "Patents", "go_uniprot", "citeseerx"));
			ArrayList<String> distribution_a = new ArrayList<String>(Arrays.asList(Distribution.Random_spatial_distributed.name(),
					Distribution.Clustered_distributed.name(), Distribution.Zipf_distributed.name()));

			for(String datasource : datasource_a)
			{
				String graph_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/new_graph.txt", datasource);
				int nodeCount = OwnMethods.GetNodeCountGeneral(graph_path);

				//			String resultpath = "/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/result/Experiment_1/query_time_feline_ratio_"+suffix+".csv";
				String resultpath = "/mnt/hgfs/Experiment_Result/GeoReach_Experiment/result/distribution/PLL.csv";			
				OwnMethods.WriteFile(resultpath, true, datasource+ "\t" + selectivity + "\tColdPostgresNeo4j\n");
				OwnMethods.WriteFile(resultpath, true, "ratio\tSpa_time\treach_time\tPLL_time\tvisit_node_count\ttrue_count\n");
				
				String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
				String convertTable_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/table.txt", datasource);
				ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
				for(String distribution : distribution_a)
				{
					String querynodeid_filepath = String.format("/mnt/hgfs/Experiment_Result/GeoReach_Experiment"
							+ "/experiment_query/%s/experiment_id.txt", datasource);
					ArrayList<Integer> nodeids = OwnMethods.readIntegerArray(querynodeid_filepath);

					int spaNodeCount = (int) (nodeCount * (100-ratio) / 100.0); 
					String queryrectangle_filepath = String.format("/mnt/hgfs/Experiment_Result/GeoReach_Experiment"
							+ "/experiment_query/distribution/%s_%s_%d_queryrect_%d.txt", datasource, distribution, ratio, (int) (spaNodeCount * selectivity));
					ArrayList<MyRectangle> queryrectangles = OwnMethods.ReadExperimentQueryRectangle(queryrectangle_filepath);

					int true_count = 0;
					int accessnodecount = 0;
					int time_PLL = 0, time_spa = 0, time_reach = 0;
					for(int i = 0;i<experiment_count;i++)
					{
						OwnMethods.Print(PostgresJDBC.StopServer(password));
						OwnMethods.Print(OwnMethods.ClearCache(password));
						OwnMethods.Print(PostgresJDBC.StartServer(password));
						OwnMethods.Print(Neo4j_Graph_Store.StartServer(db_path));

						Thread.currentThread().sleep(5000);
						String table_name = String.format("%s_%s_%d",datasource, distribution, ratio);
						SpaReachPLL PLL= new SpaReachPLL(table_name);
						Thread.currentThread().sleep(5000);

						System.out.println(i);
						int id = nodeids.get(i);
						MyRectangle queryrect = queryrectangles.get(i);
						long start = System.currentTimeMillis();
						boolean result3 = PLL.ReachabilityQuery(id, queryrect, converTable);
						long time = System.currentTimeMillis() - start;
						OwnMethods.Print(result3);
						OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", PLL.postgresql_time, PLL.neo4j_time));
						OwnMethods.Print(String.format("Time:%d", time));
						OwnMethods.Print(String.format("Locate count:%d\n", PLL.locate_count));
						if(result3)
							true_count++;

						time_PLL += time;
						accessnodecount += PLL.AccessNodeCount;
						time_reach += PLL.neo4j_time;
						time_spa += PLL.postgresql_time;

						PLL.Disconnect();
						System.out.println(Neo4j_Graph_Store.StopServer(db_path));
					}

					OwnMethods.WriteFile(resultpath, true, ratio + "\t" + time_spa/experiment_count + "\t" + time_reach/experiment_count 
							+ "\t" +time_PLL/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + true_count+"\n");
				}
				OwnMethods.WriteFile(resultpath, true, "\n");
			}	
		}
		catch (Exception e) 
		{
			e.printStackTrace();System.exit(-1);
		}
	}
	
	public static void Experiment_Selectivity_Cold()
	{
		try
		{
			int ratio = 20;
			String distribution = Distribution.Random_spatial_distributed.name();
//			ArrayList<String> datasource_a = new ArrayList<String>(Arrays.asList("uniprotenc_150m", "Patents", "go_uniprot", "citeseerx"));
			ArrayList<String> datasource_a = new ArrayList<String>(Arrays.asList("go_uniprot", "citeseerx"));

			for(String datasource : datasource_a)
			{
				String entity_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/%s/%s/new_entity.txt", 
						datasource, distribution, ratio);
				ArrayList<Entity> entities = OwnMethods.ReadEntity((String)entity_path);
				int spaNodeCount = OwnMethods.GetSpatialEntityCount(entities);
				
				String resultpath = "/mnt/hgfs/Experiment_Result/GeoReach_Experiment/result/selectivity/PLL_querytime.csv";
				String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
				
				OwnMethods.WriteFile(resultpath, true, datasource+ "\t" + ratio + "\tColdPostgresNeo4j\n");
				OwnMethods.WriteFile(resultpath, true, "selectivity\tSpa_time\treach_time\tPLL_time\tvisit_node_count\ttrue_count\n");

				String convertTable_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/table.txt", datasource);
				ArrayList<Integer> converTable = ReadConvertTable(convertTable_path);
				for(double selectivity = 0.0001; selectivity < 0.2; selectivity *= 10)
				{
					String querynodeid_filepath = String.format("/mnt/hgfs/Experiment_Result/GeoReach_Experiment"
							+ "/experiment_query/%s/experiment_id.txt", datasource);
					ArrayList<Integer> nodeids = OwnMethods.readIntegerArray(querynodeid_filepath);

					String queryrectangle_filepath = String.format("/mnt/hgfs/Experiment_Result/"
							+ "GeoReach_Experiment/experiment_query/"
							+ "selectivity/%s_%s_%d_queryrect_%d.txt", 
							datasource, distribution, ratio, (int) (spaNodeCount * selectivity));
					ArrayList<MyRectangle> queryrectangles = OwnMethods.ReadExperimentQueryRectangle(queryrectangle_filepath);

					int true_count = 0;

					int accessnodecount = 0;
					int time_PLL = 0, time_spa = 0, time_reach = 0;
					int experiment_count = datasource.equals("uniprotenc_150m") && selectivity > 0.01 ? 1:3;
					for(int i = 0;i<experiment_count;i++)
					{
						OwnMethods.Print(PostgresJDBC.StopServer(password));
						OwnMethods.Print(OwnMethods.ClearCache(password));
						OwnMethods.Print(PostgresJDBC.StartServer(password));
						OwnMethods.Print(Neo4j_Graph_Store.StartServer(db_path));
						
						Thread.currentThread().sleep(5000);
						
						String table_name = String.format("%s_%s_%d",datasource, distribution, ratio);
						SpaReachPLL PLL= new SpaReachPLL(table_name);
						
						Thread.currentThread().sleep(5000);
						
						System.out.println(i);
						int id = nodeids.get(i);
						MyRectangle queryrect = queryrectangles.get(i);
						
						long start = System.currentTimeMillis();
						boolean result3 = PLL.ReachabilityQuery(id, queryrect, converTable);
						long time = System.currentTimeMillis() - start;
						System.out.println(result3);
						OwnMethods.Print(String.format("Time:%d", time));
						if(result3)
							true_count++;

						time_PLL += time;
						time_spa += PLL.postgresql_time;
						time_reach += PLL.neo4j_time;
						accessnodecount += PLL.AccessNodeCount;
						
						PLL.Disconnect();
						System.out.println(Neo4j_Graph_Store.StopServer(db_path));
					}
					OwnMethods.WriteFile(resultpath, true, selectivity + "\t" + time_spa/experiment_count + "\t" + time_reach/experiment_count 
							+ "\t" +time_PLL/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + true_count + "\t\n");
				}
				OwnMethods.WriteFile(resultpath, true, "\n");
			}		
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();System.exit(-1);
		}
	}

	public static void app()
	{
		int start = 153933;
		MyRectangle query_rect = new MyRectangle(-115.487384,	36.047015,	-115.095464,	36.120295);

		ArrayList<Integer> converTable = ReadConvertTable("/home/yuhansun/Documents/share/Real_Data/Yelp/Random_spatial_distributed/2/table.txt");
		SpaReachPLL pll = new SpaReachPLL("Yelp_2");
		boolean result = pll.ReachabilityQuery(start, query_rect, converTable);
		OwnMethods.Print(result);
	}

	public static void main(String[] args) {
		//		app();
//		LoadData();
		//		LoadDataYelp();
		//		Experiment();
//		ExperimentColdPostgresNeo4j(2);
		//		Experiment_Ratio();
		//		ExperimentCorrectness();
//		Experiment_Ratio_Cold();
		Experiment_Selectivity_Cold();
//		Experiment_Distribution_Cold();
	}
}