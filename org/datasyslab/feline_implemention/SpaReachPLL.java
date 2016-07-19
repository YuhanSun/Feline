package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.impl.util.Access;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.WebResource;

public class SpaReachPLL{

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
			if(inserter!=null)
				inserter.shutdown();
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
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
		datasource_a.add("go_uniprot");
//		datasource_a.add("Patents");
		datasource_a.add("uniprotenc_150m");

		for(String datasource: datasource_a)
		{
			String from_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/reachFromIndex.txt", datasource);
			String to_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/reachToIndex.txt", datasource);
			String table_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/table.txt", datasource);
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL/data/graph.db", datasource);
			LoadData(from_filepath, to_filepath, table_filepath, db_path);
		}
		
	}
	
	public ResultSet RangeQuery(MyRectangle rect)
	{
		String query = String.format("select id from %s where %s <@ box '((%f,%f),(%f,%f))'", tablename, location_columnname, rect.min_x, rect.min_y, rect.max_x, rect.max_y);
		ResultSet resultSet = postgresJDBC.Execute(query);
		return resultSet;
	}
	
	public boolean ReachabilityQuery(int start_id, MyRectangle rect) 
	{
		try
		{
			postgresql_time = 0;
			neo4j_time = 0;
			AccessNodeCount = 0;
			
			long start = System.currentTimeMillis();
			ResultSet resultSet = this.RangeQuery(rect);
			postgresql_time+=System.currentTimeMillis() - start;
			if(!resultSet.next())
				return false;
			else
				resultSet.previous();

			String query = "match (n:Reachability_Index) where id(n) = " + start_id + " return n";
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

			int bulksize = 500;
			int i = 0;
			while(resultSet.next())
			{				
				if(i == 0)
				{
					query = "match (n:Reachability_Index) where id(n) in ["+resultSet.getString("id").toString();
					i++;
					AccessNodeCount+=1;
					continue;
				}


				if(i == bulksize-1)
				{
					query += (","+resultSet.getString("id").toString()+"] return n");
					AccessNodeCount+=1;
					Neo4jAccessCount+=1;
					i = 0;

					start = System.currentTimeMillis();
					result = neo4j_Graph_Store.Execute(query);
					jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
					neo4j_time+=System.currentTimeMillis() - start;

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
				}
				else
				{
					query+= ("," + resultSet.getString("id").toString());
					i++;
					AccessNodeCount+=1;
				}
			}

			if(i!=0)
			{
				query+="] return n";
				Neo4jAccessCount+=1;

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
			//				int target_id = Integer.parseInt(rs.getObject("id").toString());
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
			return false;
		}
	}
	
	public static void Experiment()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//				datasource_a.add("go_uniprot");
				datasource_a.add("Patents");
				datasource_a.add("uniprotenc_150m");

		String suffix = "random";

		for(int name_index = 0;name_index<datasource_a.size();name_index++)
		{
			String datasource = datasource_a.get(name_index);
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_PLL_"+suffix+".csv";
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
			{
//				for(int ratio = 20;ratio<=20;ratio+=20)
				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tPLL_time\tvisit_node_count\ttrue_count\n");

					ArrayList<Long> tmp_al = OwnMethods.ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Long absolute_id = (tmp_al.get(i));
						String x = absolute_id.toString();
						experiment_id_al.add(x);
					}

					ArrayList<Double> a_x = new ArrayList<Double>();
					ArrayList<Double> a_y = new ArrayList<Double>();

					Random r = new Random();

					double selectivity = 0.0001;
					double spatial_total_range = 1000;
					boolean isrun = true;
					boolean isbreak = false;
					int experiment_count = 10;
//					int experiment_count = 5;
					{
						while(selectivity<=0.11)
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
									boolean result3 = PLL.ReachabilityQuery(id, query_rect);
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
							if(time_PLL/experiment_count >= 10000 && experiment_count == 100)
								experiment_count =3;

							PLL.Disconnect();
							System.out.println(Neo4j_Graph_Store.StopServer(db_path));

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
	
	public static void Experiment_Ratio()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//				datasource_a.add("go_uniprot");
//				datasource_a.add("Patents");
				datasource_a.add("uniprotenc_150m");

		String suffix = "random";

		for(int name_index = 0;name_index<datasource_a.size();name_index++)
		{
			String datasource = datasource_a.get(name_index);
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_PLL_"+suffix+".csv";
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_PLL", datasource);
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
			{
				for(int ratio = 80;ratio>=20;ratio-=20)
//				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tPLL_time\tvisit_node_count\ttrue_count\n");

					ArrayList<Long> tmp_al = OwnMethods.ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Long absolute_id = (tmp_al.get(i));
						String x = absolute_id.toString();
						experiment_id_al.add(x);
					}

					ArrayList<Double> a_x = new ArrayList<Double>();
					ArrayList<Double> a_y = new ArrayList<Double>();

					Random r = new Random();

					double selectivity = 0.0001;
					double spatial_total_range = 1000;
					boolean isrun = true;
					boolean isbreak = false;
					int experiment_count = experiment_id_al.size();
//					int experiment_count = 5;
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
									boolean result3 = PLL.ReachabilityQuery(id, query_rect);
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
							if(time_PLL/experiment_count >= 10000 && experiment_count == 500)
								experiment_count /=100;

							PLL.Disconnect();
							System.out.println(Neo4j_Graph_Store.StopServer(db_path));

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
	
	public static void main(String[] args) {
//		LoadData();
		Experiment();
	}
}