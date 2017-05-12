package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.neo4j.kernel.impl.util.dbstructure.DbStructureArgumentFormatter;


public class Experiment_feline {
	
	public static String password = "syh19910205";
	
	public static ArrayList<String> ReadExperimentNode(String datasource)
	{
		String filepath = "/home/yuhansun/Documents/share/Real_Data/"+datasource+"/experiment_id.txt";
		ArrayList<String> al = new ArrayList<String>();
		BufferedReader reader  = null;
		File file = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null)
			{
				al.add(temp);
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
		return al;
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
	
	public static void Experiment()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//				datasource_a.add("go_uniprot");
//				datasource_a.add("Patents");
//				datasource_a.add("uniprotenc_150m");

		String suffix = "random";

		{
//			String datasource = datasource_a.get(name_index);
			String datasource = "Yelp";
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_feline_"+suffix+".csv";
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
			
			String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/SCC.txt", datasource);
			String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/graph_entity.txt", datasource);
			ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);
			{
//				for(int ratio = 20;ratio<=80;ratio+=20)
				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tfeline_time\tvisit_node_count\tlocate_count\ttrue_count\n");

					ArrayList<String> tmp_al = ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Integer absolute_id = Integer.parseInt(tmp_al.get(i));
						String x = absolute_id.toString();
						experiment_id_al.add(x);
					}

					
					double selectivity = 0.000001;
					double spatial_total_range = 1000;
					boolean isrun = true;
					boolean isbreak = false;
//					int experiment_count = experiment_id_al.size();
					int experiment_count = 10;
					{
						while(selectivity<=0.11)
						{
							double rect_size = spatial_total_range * Math.sqrt(selectivity);
							OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

							int log = (int)Math.log10(selectivity);
							String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
							ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);


							int true_count = 0;
							//Feline
							OwnMethods.Print(Neo4j_Graph_Store.StopMyServer(datasource));
							OwnMethods.Print(PostgresJDBC.StopServer(password));
							OwnMethods.Print(OwnMethods.ClearCache(password));
							OwnMethods.Print(PostgresJDBC.StartServer(password));
							OwnMethods.Print(Neo4j_Graph_Store.StartMyServer(datasource));

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							String table_name = String.format("%s_random_%d",datasource, ratio);
							Feline feline = new Feline(datasource, table_name);

							int accessnodecount = 0;
							int time_feline = 0, time_spa = 0, time_reach = 0, locate_count = 0;
							for(int i = 0;i<experiment_count;i++)
							{
								
								MyRectangle query_rect = queryrectangles.get(i);

								OwnMethods.Print(i);
								int id = Integer.parseInt(experiment_id_al.get(i));
								OwnMethods.Print(id);

								try
								{
									long start = System.currentTimeMillis();
									id = refer_table.get(id);
									boolean result3 = feline.RangeReach(id, query_rect);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", feline.spa_time, feline.reach_time));
									OwnMethods.Print(String.format("Time:%d", time));
									OwnMethods.Print(String.format("Locate count:%d\n", feline.locate_count));
									if(result3)
										true_count++;
									
									accessnodecount += feline.visited_count;
									time_reach += feline.reach_time;
									time_spa += feline.spa_time;
									time_feline += time;
									locate_count += feline.locate_count;
									
								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_feline/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + locate_count / experiment_count+"\t");
							if(time_feline/experiment_count >= 100000 && experiment_count >= 5)
								experiment_count =3;

							feline.Disconnect();
							Neo4j_Graph_Store.StopMyServer(datasource);

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

		{
//			String datasource = datasource_a.get(name_index);
			String datasource = "Yelp";
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_feline_"+suffix+".csv";
			OwnMethods.WriteFile(resultpath, true, datasource+"(ColdPostgres)\n");
			
			String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/SCC.txt", datasource);
			String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/graph_entity.txt", datasource);
			ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);
			{
//				for(int ratio = 20;ratio<=80;ratio+=20)
				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tfeline_time\tvisit_node_count\tlocate_count\ttrue_count\n");

					ArrayList<String> tmp_al = ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Integer absolute_id = Integer.parseInt(tmp_al.get(i));
						String x = absolute_id.toString();
						experiment_id_al.add(x);
					}

					
					double selectivity = 0.000001;
					double spatial_total_range = 1000;
					boolean isrun = true;
					boolean isbreak = false;
//					int experiment_count = experiment_id_al.size();
					int experiment_count = 10;
					{
						while(selectivity<=0.11)
						{
							double rect_size = spatial_total_range * Math.sqrt(selectivity);
							OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

							int log = (int)Math.log10(selectivity);
							String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
							ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);


							int true_count = 0;
							//Feline
							OwnMethods.Print(Neo4j_Graph_Store.StopMyServer(datasource));
							OwnMethods.Print(OwnMethods.ClearCache(password));
							OwnMethods.Print(Neo4j_Graph_Store.StartMyServer(datasource));

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							String table_name = String.format("%s_random_%d",datasource, ratio);

							int accessnodecount = 0;
							int time_feline = 0, time_spa = 0, time_reach = 0, locate_count = 0;
							for(int i = 0;i<experiment_count;i++)
							{
								
								MyRectangle query_rect = queryrectangles.get(i);

								OwnMethods.Print(i);
								int id = Integer.parseInt(experiment_id_al.get(i));
								OwnMethods.Print(id);

								try
								{
//									OwnMethods.Print(PostgresJDBC.StopServer(password));
//									OwnMethods.Print(OwnMethods.ClearCache(password));
//									OwnMethods.Print(PostgresJDBC.StartServer(password));
									
									PostgresJDBC.StopServer(password);
									OwnMethods.ClearCache(password);
									PostgresJDBC.StartServer(password);
									
									Feline feline = new Feline(datasource, table_name);
									
									long start = System.currentTimeMillis();
									id = refer_table.get(id);
									boolean result3 = feline.RangeReach(id, query_rect);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", feline.spa_time, feline.reach_time));
									OwnMethods.Print(String.format("Time:%d", time));
									OwnMethods.Print(String.format("Locate count:%d\n", feline.locate_count));
									if(result3)
										true_count++;
									
									accessnodecount += feline.visited_count;
									time_reach += feline.reach_time;
									time_spa += feline.spa_time;
									time_feline += time;
									locate_count += feline.locate_count;
									
									feline.Disconnect();
									
								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_feline/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + locate_count / experiment_count+"\t");
//							if(time_feline/experiment_count >= 10000 && experiment_count == 10)
//								experiment_count =3;

							Neo4j_Graph_Store.StopMyServer(datasource);

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

		{
//			String datasource = datasource_a.get(name_index);
			String datasource = "Yelp";
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_feline_"+suffix+".csv";
			OwnMethods.WriteFile(resultpath, true, datasource+"(ColdPostgresNeo4j)\n");
			
			String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/SCC.txt", datasource);
			String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/graph_entity.txt", datasource);
			ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);
			{
//				for(int ratio = 20;ratio<=80;ratio+=20)
				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tfeline_time\tvisit_node_count\tlocate_count\ttrue_count\n");

					ArrayList<String> tmp_al = ReadExperimentNode(datasource);
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
//					int experiment_count = experiment_id_al.size();
					int experiment_count = 100;
					{
						while(selectivity<=0.11)
						{
							OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

							int log = (int)Math.log10(selectivity);
							String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
							ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);


							int true_count = 0;
							//Feline
							String table_name = String.format("%s_random_%d",datasource, ratio);

							int accessnodecount = 0;
							int time_feline = 0, time_spa = 0, time_reach = 0, locate_count = 0;
							for(int i = 0;i<experiment_count;i++)
							{
								
								MyRectangle query_rect = queryrectangles.get(i);

								OwnMethods.Print(i);
								int id = Integer.parseInt(experiment_id_al.get(i));
								OwnMethods.Print(id);

								try
								{
//									OwnMethods.Print(PostgresJDBC.StopServer(password));
//									OwnMethods.Print(OwnMethods.ClearCache(password));
//									OwnMethods.Print(PostgresJDBC.StartServer(password));
									
									PostgresJDBC.StopServer(password);
									OwnMethods.ClearCache(password);
									Neo4j_Graph_Store.StartMyServer(datasource);
									PostgresJDBC.StartServer(password);

									try {
										Thread.currentThread().sleep(5000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}

									Feline feline = new Feline(datasource, table_name);
									
									long start = System.currentTimeMillis();
									id = refer_table.get(id);
									boolean result3 = feline.RangeReach(id, query_rect);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", feline.spa_time, feline.reach_time));
									OwnMethods.Print(String.format("Time:%d", time));
									OwnMethods.Print(String.format("Locate count:%d\n", feline.locate_count));
									if(result3)
										true_count++;
									
									accessnodecount += feline.visited_count;
									time_reach += feline.reach_time;
									time_spa += feline.spa_time;
									time_feline += time;
									locate_count += feline.locate_count;
									
									feline.Disconnect();
									Neo4j_Graph_Store.StopMyServer(datasource);
								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_feline/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + locate_count / experiment_count+"\t");
//							if(time_feline/experiment_count >= 10000 && experiment_count == 10)
//								experiment_count =3;
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
		ArrayList<String> datasource_a = new ArrayList<String>();
		String distribution = "Random_spatial_distributed";
//		int target_folder = 2;
//		datasource_a.add("citeseerx");
//				datasource_a.add("go_uniprot");
//				datasource_a.add("Patents");
//				datasource_a.add("uniprotenc_150m");

		{
//			String datasource = datasource_a.get(name_index);
			String datasource = "Gowalla";
			String database_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_feline_%d", datasource, target_folder);
			String resultpath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/result/%s_Feline_querytime.csv", datasource);
			OwnMethods.WriteFile(resultpath, true, datasource + "\t"+ target_folder +"\t(ColdPostgresNeo4j)\n");
			
			String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/SCC.txt", datasource, distribution, target_folder);
			String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/graph_entity_newformat.txt", datasource, distribution, target_folder);
			ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);
			{
				{
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tfeline_time\tvisit_node_count\tlocate_count\ttrue_count\n");

					String querynodeid_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/experiment_id.txt", datasource, distribution, target_folder);
					ArrayList<String> experiment_id_al = OwnMethods.ReadExperimentNodeGeneral(querynodeid_filepath);
					
					double selectivity = 0.000001;
//					double selectivity = 0.01;
					
					boolean isrun = true;
					boolean isbreak = false;
//					int experiment_count = experiment_id_al.size();
					int experiment_count = 20;
					{
						while(selectivity<=0.002)
						{
							OwnMethods.WriteFile(resultpath, true, selectivity+"\t");

							int log = (int)Math.log10(selectivity);
							String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
							ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);


							int true_count = 0;
							//Feline
							String table_name = String.format("%s_%d",datasource, target_folder);

							int accessnodecount = 0;
							int time_feline = 0, time_spa = 0, time_reach = 0, locate_count = 0;
							for(int i = 0;i<experiment_count;i++)
							{
								
								MyRectangle query_rect = queryrectangles.get(i);

								OwnMethods.Print(i);
								int id = Integer.parseInt(experiment_id_al.get(i));
								OwnMethods.Print(id);

								try
								{
									PostgresJDBC.StopServer(password);
									OwnMethods.ClearCache(password);
									OwnMethods.Print(database_path);
									OwnMethods.Print(Neo4j_Graph_Store.StartServer(database_path));
									PostgresJDBC.StartServer(password);

									try {
										Thread.currentThread().sleep(2000);
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}

									int dag_node_count = OwnMethods.GetNodeCountGeneral(String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/graph_dag_newformat.txt", datasource, distribution, target_folder));
									Feline feline = new Feline(datasource, table_name, dag_node_count);
									
									long start = System.currentTimeMillis();
									id = refer_table.get(id);
									boolean result3 = feline.RangeReach(id, query_rect);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", feline.spa_time, feline.reach_time));
									OwnMethods.Print(String.format("Time:%d", time));
									OwnMethods.Print(String.format("Locate count:%d\n", feline.locate_count));
									if(result3)
										true_count++;
									
									
									accessnodecount += feline.visited_count;
									time_reach += feline.reach_time;
									time_spa += feline.spa_time;
									time_feline += time;
									locate_count += feline.locate_count;
									
									feline.Disconnect();
									Neo4j_Graph_Store.StopServer(database_path);
								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_feline/experiment_count+"\t" + accessnodecount/experiment_count + "\t" + locate_count / experiment_count+"\t");
//							if(time_feline / experiment_count >= 50000 && experiment_count >=10)
//								experiment_count = 10;
//							if(time_feline/experiment_count >= 100000 && experiment_count >= 5)
//								experiment_count =3;
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
	
	public static void ExperimentColdPostgresNeo4jCorrectness()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		String distribution = "Random_spatial_distributed";
		int target_folder = 2;

		{
			String datasource = "Yelp";
			String database_path = String.format("/home/yuhansun/Documents/Real_data/%s/neo4j-community-2.3.3_feline_%d", datasource, target_folder);
			
			String SCC_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/SCC.txt", datasource, distribution, target_folder);
			String original_graph_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/graph_entity_newformat.txt", datasource, distribution, target_folder);
			ArrayList<Integer> refer_table = OwnMethods.ReadSCC(SCC_filepath, original_graph_path);
			{
				{

					String querynodeid_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/experiment_id.txt", datasource, distribution, target_folder);
					ArrayList<String> experiment_id_al = OwnMethods.ReadExperimentNodeGeneral(querynodeid_filepath);
					
					double selectivity = 0.00001;
					boolean isrun = true;
					boolean isbreak = false;
//					int experiment_count = experiment_id_al.size();
					int experiment_count = 20;
					{
//						while(selectivity<=0.11)
						{
							int log = (int)Math.log10(selectivity);
							String queryrectangle_filepath = String.format("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/experiment_query/%s_%d.txt", datasource, log);
							ArrayList<MyRectangle> queryrectangles = ReadExperimentQueryRectangle(queryrectangle_filepath);

							int true_count = 0;
							//Feline
							String table_name = String.format("%s_%d",datasource, target_folder);

							int accessnodecount = 0;
							int time_feline = 0, time_spa = 0, time_reach = 0, locate_count = 0;
							int dag_node_count = OwnMethods.GetNodeCountGeneral(String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/graph_dag_newformat.txt", datasource, distribution, target_folder));
							Feline feline = new Feline(datasource, table_name, dag_node_count);
							for(int i = 0;i<experiment_count;i++)
							{
								
								MyRectangle query_rect = queryrectangles.get(i);

								OwnMethods.Print(i);
								int id = Integer.parseInt(experiment_id_al.get(i));
								OwnMethods.Print(id);

								try
								{
									long start = System.currentTimeMillis();
									id = refer_table.get(id);
									boolean result3 = feline.RangeReach(id, query_rect);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", feline.spa_time, feline.reach_time));
									OwnMethods.Print(String.format("Time:%d", time));
									OwnMethods.Print(String.format("Locate count:%d\n", feline.locate_count));
									if(result3)
										true_count++;
									OwnMethods.WriteFile("/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/result/feline_result.txt", true, result3 + "\n");

									accessnodecount += feline.visited_count;
									time_reach += feline.reach_time;
									time_spa += feline.spa_time;
									time_feline += time;
									locate_count += feline.locate_count;
								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							feline.Disconnect();
						}
					}
				}
			}
		}	
	}
	
	public static void Experiment_Ratio()
	{
		ArrayList<String> datasource_a = new ArrayList<String>();
		datasource_a.add("citeseerx");
				datasource_a.add("go_uniprot");
				datasource_a.add("Patents");
//				datasource_a.add("uniprotenc_150m");

		String suffix = "random";

		for(int name_index = 0;name_index<datasource_a.size();name_index++)
		{
			String datasource = datasource_a.get(name_index);
//			String datasource = "Patents";
			String resultpath = "/home/yuhansun/Documents/share/Real_Data/GeoReach_Experiment/result/Experiment_1/query_time_feline_ratio_"+suffix+".csv";
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
			{
				for(int ratio = 20;ratio<=80;ratio+=20)
//				int ratio = 80;
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\tSpa_time\treach_time\tfeline_time\tvisit_node_count\ttrue_count\n");

					ArrayList<String> tmp_al = ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Integer absolute_id = Integer.parseInt(tmp_al.get(i));
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
							//Feline
							OwnMethods.Print(PostgresJDBC.StopServer(password));
							OwnMethods.Print(OwnMethods.ClearCache(password));
							OwnMethods.Print(PostgresJDBC.StartServer(password));
							OwnMethods.Print(Neo4j_Graph_Store.StartMyServer(datasource));

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							String table_name = String.format("%s_random_%d",datasource, ratio);
							Feline feline = new Feline(datasource, table_name);

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							int accessnodecount = 0;
							int time_feline = 0, time_spa = 0, time_reach = 0;
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
									boolean result3 = feline.RangeReach(id, query_rect);
									long time = System.currentTimeMillis() - start;
									OwnMethods.Print(result3);
									OwnMethods.Print(String.format("Postgres Time:%d\tNeo4j Time:%d", feline.spa_time, feline.reach_time));
									OwnMethods.Print(String.format("Time:%d", time));
									OwnMethods.Print(String.format("Locate count:%d\n", feline.locate_count));
									if(result3)
										true_count++;
									
									time_feline += time;
									accessnodecount += feline.visited_count;
									time_reach += feline.reach_time;
									time_spa += feline.spa_time;
								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.WriteFile(resultpath, true, time_spa/experiment_count + "\t" + time_reach/experiment_count + "\t" +time_feline/experiment_count+"\t" + accessnodecount/experiment_count + "\t");
//							if(time_feline/experiment_count >= 10000 && experiment_count == 500)
//								experiment_count /=100;

							feline.Disconnect();
							System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
							
							if(isbreak)
								break;
							OwnMethods.WriteFile(resultpath, true, true_count+"\n");
//							selectivity*=10;
						}
					}
				}
				OwnMethods.WriteFile(resultpath, true, "\n");
			}


			OwnMethods.WriteFile(resultpath, true, "\n");
		}	
	}

	public static void main(String[] args) 
	{
//			Experiment_Ratio();
//		Experiment();
		ExperimentColdPostgresNeo4j(2);
//		ExperimentColdPostgresNeo4jCorrectness();
	}

}
