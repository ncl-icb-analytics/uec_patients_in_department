SELECT
	--Patient Info
	 [ROWNUMBER_ID] AS [pid]
	--Site info
	,[attendance.location.hes_provider_3] AS [org_code]
	,[attendance.location.site] AS [site_code]
	--Arrival Time
	,[attendance.arrival.date] AS [arrival_date]
	,[attendance.arrival.time] AS [arrival_time]
	--Departure Time
	,[attendance.departure.date] AS [departure_date]
	,[attendance.departure.time] AS [departure_time]

FROM [Data_Store_SUS_Unified].[ECDS].[emergency_care] ec

WHERE 
	--Filter sites to NMUH, WH, UCLH, RAL01 (RFL), RAL26 (RFL)
	( 
		[attendance.location.hes_provider_3] IN ('RAP', 'RKE', 'RRV') OR
		[attendance.location.site] IN ('RAL01', 'RAL26')
	)
	--Filter arrival modes to: Emergency road ambulance, Emergency road ambulance with medical escort, and Non-emergency road ambulance
	AND [attendance.arrival.arrival_mode.code] IN ('1048031000000100', '1048041000000109', '1048021000000102')

	AND [attendance.arrival.date] IS NOT NULL
	AND [attendance.arrival.time] IS NOT NULL
	AND [attendance.departure.date] IS NOT NULL
	AND [attendance.departure.time] IS NOT NULL

	AND [attendance.arrival.date] >= '2019-04-01 00:00:00'