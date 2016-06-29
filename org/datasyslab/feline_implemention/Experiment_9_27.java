package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


public class Experiment_9_27 {

	public static ArrayList<String> ReadExperimentNode(String datasource)
	{
		String filepath = "/home/yuhansun/Documents/Real_data/"+datasource+"/experiment_id.txt";
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
	
	public static void main(String[] args) 
	{
		MyRectangle rect = new MyRectangle(0,0,1000,1000);
		Config p_con = new Config();
		ArrayList<String> datasource_a = new ArrayList<String>();
//		datasource_a.add("citeseerx");
//		datasource_a.add("go_uniprot");
		datasource_a.add("Patents");
//		datasource_a.add("uniprotenc_150m");
		
//		String suffix = args[0];
		String suffix = "random";
		
		for(int name_index = 0;name_index<datasource_a.size();name_index++)
		{
			String datasource = datasource_a.get(name_index);
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_4_25_"+suffix+".csv";
			int node_count = OwnMethods.GetNodeCount(datasource);
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
			{
				for(int ratio = 20;ratio<=80;ratio+=20)
				{
					OwnMethods.WriteFile(resultpath, true, ratio+"\n");
					OwnMethods.WriteFile(resultpath, true, "spatial_range\ttra_time\tspareach_time\ttrue_count\taccess_count\n");
					long graph_size = OwnMethods.GetNodeCount(datasource);
					
					ArrayList<String> tmp_al = ReadExperimentNode(datasource);
					ArrayList<String> experiment_id_al = new ArrayList<String>();
					for(int i = 0;i<tmp_al.size();i++)
					{
						Integer absolute_id = Integer.parseInt(tmp_al.get(i))+node_count;
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
					{
						while(selectivity<=1)
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
							ArrayList<Boolean> tra_result = new ArrayList<Boolean>(); 
							ArrayList<Boolean> spa_result = new ArrayList<Boolean>();	
							//Traversal
							System.out.println(OwnMethods.RestartNeo4jClearCache(datasource));
							System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							Traversal tra = new Traversal(suffix, ratio);

							try {
								Thread.currentThread().sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}

							int accessnodecount = 0;
							int time_tra = 0;
							for(int i = 0;i<experiment_id_al.size();i++)
							{
								double x = a_x.get(i);
								double y = a_y.get(i);
								MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);

								System.out.println(i);
								int id = Integer.parseInt(experiment_id_al.get(i));
								System.out.println(id);

								try
								{
									tra.VisitedVertices.clear();
									long start = System.currentTimeMillis();
									boolean result3 = tra.ReachabilityQuery(id, query_rect);
									time_tra += System.currentTimeMillis() - start;
									System.out.println(result3);
									if(result3)
										true_count++;
									accessnodecount+=tra.VisitedVertices.size();
								}
								catch(Exception e)
								{
									e.printStackTrace();
									OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
									i = i-1;
								}						
							}
							OwnMethods.WriteFile(resultpath, true, time_tra/(experiment_id_al.size())+"\t");
															
							//SpaReach
							if(isrun)
							{
								int time_spareach = 0;
								System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
								System.out.println(PostgresJDBC.StopServer());
								System.out.println(OwnMethods.ClearCache());
								System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
								System.out.println(PostgresJDBC.StartServer());
								System.out.println(Neo4j_Graph_Store.StartMyServer(datasource));
								try {
									Thread.currentThread().sleep(5000);
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
								String R_Tree_suffix = "";
								if(suffix.equals("random"))
									R_Tree_suffix = "";
								Spatial_Reach_Index spareach = new Spatial_Reach_Index(datasource + "_Random_" + ratio, R_Tree_suffix);
								try {
									Thread.currentThread().sleep(5000);
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
								for(int i = 0;i<tmp_al.size();i++)
								{
									double x = a_x.get(i);
									double y = a_y.get(i);
									MyRectangle query_rect = new MyRectangle(x, y, x + rect_size, y + rect_size);

									System.out.println(i);
									int id = Integer.parseInt(tmp_al.get(i));
									System.out.println(id);

									try
									{
										long start = System.currentTimeMillis();
										boolean result2 = spareach.ReachabilityQuery(id, query_rect);
										time_spareach += (System.currentTimeMillis() - start);
										System.out.println(result2);
										spa_result.add(result2);
									}
									catch(Exception e)
									{
										e.printStackTrace();
										OwnMethods.WriteFile("/home/yuhansun/Documents/Real_data/"+datasource+"/error.txt", true, e.getMessage().toString()+"\n");
										i = i-1;
									}						
								}
								spareach.Disconnect();
								
								for(int i = 0;i<tmp_al.size();i++)
								{
									if(tra_result.get(i) == spa_result.get(i))
										continue;
									isbreak = true;
									break;
								}
								
								if(time_spareach/(tmp_al.size())>10000)
									isrun = false;
								if(isbreak)
									break;
								OwnMethods.WriteFile(resultpath, true, time_spareach/(tmp_al.size())+"\t");
								OwnMethods.WriteFile(resultpath, true, true_count+"\t"+accessnodecount+"\n");
							}
							else
								OwnMethods.WriteFile(resultpath, true, "\t");

							selectivity*=10;
						}
					}
					OwnMethods.WriteFile(resultpath, true, "\n");
				}
			}
			
			
			OwnMethods.WriteFile(resultpath, true, "\n");
			System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
		}		
	}

}
