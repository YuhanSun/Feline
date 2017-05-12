package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


public class Experiment_feline {
	
	public static String password = "syh19910205";
	
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
//			String datasource = "Patents";
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_feline_"+suffix+".csv";
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
			{
//				for(int ratio = 20;ratio<=80;ratio+=20)
				int ratio = 80;
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
					int experiment_count = experiment_id_al.size();
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

							String table_name = String.format("feline_%s_random_%d",datasource, ratio);
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
									time_feline += System.currentTimeMillis() - start;
									System.out.println(result3);
									OwnMethods.Print(time_feline);
									if(result3)
										true_count++;
									
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
							if(time_feline/experiment_count >= 10000 && experiment_count == 500)
								experiment_count /=100;

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
//			String datasource = "Patents";
			String resultpath = "/home/yuhansun/Documents/Real_data/query_time_feline_ratio_"+suffix+".csv";
			OwnMethods.WriteFile(resultpath, true, datasource+"\n");
			{
				for(int ratio = 80;ratio>=20;ratio-=20)
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
					
					double selectivity;
					if(datasource.equals("Patents"))
						selectivity = 0.001;
					else
						selectivity = 0.0001;
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
									System.out.println(result3);
									OwnMethods.Print(time);
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
							if(time_feline/experiment_count >= 10000 && experiment_count == 500)
								experiment_count /=100;

							feline.Disconnect();
							System.out.println(Neo4j_Graph_Store.StopMyServer(datasource));
							
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

	public static void main(String[] args) 
	{
//			Experiment_Ratio();
		System.out.println("test");
	}

}
