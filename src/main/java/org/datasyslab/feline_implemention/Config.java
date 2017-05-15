package org.datasyslab.feline_implemention;

public class Config	{
	public static enum Distribution 
	{
		Random_spatial_distributed, Clustered_distributed, Zipf_distributed
	}
	
	private String SERVER_ROOT_URI = "http://localhost:7474/db/data";
	
//	private String suffix = "";
	private String longitude_property_name = "lon";
	private String latitude_property_name = "lat";
	
//	private String RMBR_minx_name = "RMBR_minx";
//	private String RMBR_miny_name = "RMBR_miny";
//	private String RMBR_maxx_name = "RMBR_maxx";
//	private String RMBR_maxy_name = "RMBR_maxy";
	
	public String GetServerRoot() {
		return SERVER_ROOT_URI;
	}
	
	public String GetLongitudePropertyName() {
//		return longitude_property_name + suffix;
		return longitude_property_name;
	}
	
	public String GetLatitudePropertyName() {
//		return latitude_property_name + suffix;
		return latitude_property_name;
	}
	
//	public String GetRMBR_minx_name()
//	{
//		return RMBR_minx_name + suffix;
//	}
//	
//	public String GetRMBR_miny_name()
//	{
//		return RMBR_miny_name + suffix;
//	}
//	
//	public String GetRMBR_maxx_name()
//	{
//		return RMBR_maxx_name + suffix;
//	}
//	
//	public String GetRMBR_maxy_name()
//	{
//		return RMBR_maxy_name + suffix;
//	}
//	
//	public String GetSuffix()
//	{
//		return suffix;
//	}
}