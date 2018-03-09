package org.tcb.airplanePerformance.batchProcessing;

public enum ColumnName {
	
	AIRPORTS("iata","airport","city","state","country","lat","long"),
	//
	OTP("Year","Month","DayofMonth","DayOfWeek","DepTime","CRSDepTime","ArrTime","CRSArrTime",
			"UniqueCarrier","FlightNum","TailNum","ActualElapsedTime","CRSElapsedTime","AirTime",
			"ArrDelay","DepDelay","Origin","Dest","Distance","TaxiIn","TaxiOut","Cancelled",
			"CancellationCode","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"),
	CARRIERS("Code","Description"), //
	PLANEDATE ("tailnum","type","manufacturer","issue_date","model","status","aircraft_type","engine_type","year");
	private String[] columns;
	
	private ColumnName(String... columns) {
		this.columns = columns;
	}
	
	public String[] getColumns() {
		return columns;
	}
}
