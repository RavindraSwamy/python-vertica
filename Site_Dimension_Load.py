#! /usr/bin/python

# Import Vertica Package
import Vertica
import Logger 

log=Logger.Logger(file='site.log',dir='/home/akashg/python_scripts/log')

# Create a connector for Vertica
DB=Vertica.Vertica(verbose=True)

# -- Check for connection Failure

# -- Step 0: Truncate all temporary tables.
SQL_STMT="TRUNCATE TABLE DEV_DIM.SITE_DIM_SOURCE_DATA"
DB.execute(SQL_STMT);

# -- Step 1: (a.) To remove absolute duplicate data 
# -- (b.) To generate rank on the Partner_ID order by Created_Date
SQL_STMT="""INSERT INTO DEV_DIM.SITE_DIM_SOURCE_DATA
SELECT
      Indus_Site_ID
    , Site_Name
    , Site_Type
    , Comfort_Factor
    , Feeder_Id
    , Shelter_Length
    , Shelter_Width
    , Shelter_Height
    , Shelter_Size
    , Sanctioned_Load
    , Site_Equipment
    , Site_Location
    , Site_Escalation_Type
    , Site_Access_Type
    , Approved_Extra_Storage
    , Anchor_Customer_Key
    , Tower_Type
    , Tenancy_Count
    , Green_Site
    , Green_Site_Date
    , FCU_Only_Site
    , Site_Network_Type
    , Create_Date
    , Theft_Prone_Indicator
    , Site_Tower_Type
    , Rank_Val
FROM
(
    SELECT
          P.Indus_Site_ID
        , P.Site_Name
        , P.Site_Type
        , P.Comfort_Factor
        , P.Feeder_Id
        , P.Shelter_Length
        , P.Shelter_Width
        , P.Shelter_Height
        , P.Shelter_Size
        , P.Sanctioned_Load
        , P.Site_Equipment
        , P.Site_Location
        , P.Site_Escalation_Type
        , P.Site_Access_Type
        , P.Approved_Extra_Storage
        , P.Anchor_Customer_Key
        , P.Tower_Type
        , P.Tenancy_Count
        , P.Green_Site
        , P.Green_Site_Date
        , P.FCU_Only_Site
        , P.Site_Network_Type
        , P.Create_Date
        , P.Theft_Prone_Indicator
        , P.Site_Tower_Type
        , ROW_NUMBER() OVER (PARTITION BY P.Indus_Site_ID ORDER BY P.Create_Date) AS Rank_Val
    FROM
    (
        SELECT
              Indus_Site_ID
            , Site_Name
            , Site_Type
            , Comfort_Factor
            , Feeder_Id
            , Shelter_Length
            , Shelter_Width
            , Shelter_Height
            , Shelter_Size
            , Sanctioned_Load
            , Site_Equipment
            , Site_Location
            , Site_Escalation_Type
            , Site_Access_Type
            , Approved_Extra_Storage
            , Anchor_Customer_Key
            , Tower_Type
            , Tenancy_Count
            , Green_Site
            , Green_Site_Date
            , FCU_Only_Site
            , Site_Network_Type
            , Create_Date
            , Theft_Prone_Indicator
            , Site_Tower_Type
            , ROW_NUMBER() OVER (PARTITION BY Indus_Site_ID,MD5(TRIM(Site_Name)||TRIM(Site_Type)||NVL(TRIM(Comfort_Factor),'')||TRIM(Feeder_Id)||NVL(Shelter_Length,0)||NVL(Shelter_Width,0)||NVL(Shelter_Height,0)||NVL(Shelter_Size,0)||NVL(Sanctioned_Load,0)||NVL(TRIM(Site_Equipment),'')||NVL(TRIM(Site_Location),'')||NVL(TRIM(Site_Escalation_Type),'')||NVL(TRIM(Site_Access_Type),'')||NVL(Approved_Extra_Storage,'F')||Anchor_Customer_Key||NVL(TRIM(Tower_Type),'')||NVL(Tenancy_Count,0)||NVL(TRIM(Green_Site),'')||NVL(Green_Site_Date,'9999-12-31')||FCU_Only_Site||NVL(TRIM(Site_Network_Type),'')) ORDER BY create_date) AS ROW_NUM
        FROM DEV_DIM.STG_SITE_DIMENSION
    ) AS P
    WHERE P.ROW_NUM=1
) AS Q"""

DB.execute(SQL_STMT);

# -- Step 2: Find Max of Row_Number for looping.
DB.execute("SELECT MAX(RANK_VAL) FROM DEV_DIM.SITE_DIM_SOURCE_DATA");
RES=DB.fetch(1)
MAX_RANK=RES[0]

RANK_COUNTER_VARIABLE=1

while ( RANK_COUNTER_VARIABLE <= MAX_RANK ):

	# -- Step 2: Looping using Shell Script. Assuming i have already captured that and create a temp table with RANK=1
	SQL_STMT="TRUNCATE TABLE DEV_DIM.SITE_DIM_SOURCE_DATA_2;"
	DB.execute(SQL_STMT);

	SQL_STMT="""INSERT INTO DEV_DIM.SITE_DIM_SOURCE_DATA_2
	(
	      Indus_Site_ID
	    , Site_Name
	    , Site_Type
	    , Comfort_Factor
	    , Feeder_Id
	    , Shelter_Length
	    , Shelter_Width
	    , Shelter_Height
	    , Shelter_Size
	    , Sanctioned_Load
	    , Site_Equipment
	    , Site_Location
	    , Site_Escalation_Type
	    , Site_Access_Type
	    , Approved_Extra_Storage
	    , Anchor_Customer_Key
	    , Tower_Type
	    , Tenancy_Count
	    , Green_Site
	    , Green_Site_Date
	    , FCU_Only_Site
	    , Site_Network_Type
	    , Create_Date
	    , Theft_Prone_Indicator
	    , Site_Tower_Type
	)
	SELECT
	      Indus_Site_ID
	    , Site_Name
	    , Site_Type
	    , Comfort_Factor
	    , Feeder_Id
	    , Shelter_Length
	    , Shelter_Width
	    , Shelter_Height
	    , Shelter_Size
	    , Sanctioned_Load
	    , Site_Equipment
	    , Site_Location
	    , Site_Escalation_Type
	    , Site_Access_Type
	    , Approved_Extra_Storage
	    , Anchor_Customer_Key
	    , Tower_Type
	    , Tenancy_Count
	    , Green_Site
	    , Green_Site_Date
	    , FCU_Only_Site
	    , Site_Network_Type
	    , Create_Date
	    , Theft_Prone_Indicator
	    , Site_Tower_Type
	FROM DEV_DIM.SITE_DIM_SOURCE_DATA
	WHERE Rank_Val = """ + str(RANK_COUNTER_VARIABLE)

	DB.execute(SQL_STMT);

	# -- Step 3: Generate the XREF Table
	SQL_STMT="TRUNCATE TABLE DEV_DIM.SITE_TEMP_XREF"

	DB.execute(SQL_STMT);

	SQL_STMT="""INSERT INTO DEV_DIM.SITE_TEMP_XREF
	(
	      Indus_Site_ID
	    , Site_Key
	    , OPERATION_FLAG
	    , OPERATION_TYPE\
	)
	SELECT
	      SRC.Indus_Site_ID
	    , TGT.Site_Key
	    , ( CASE WHEN TGT.Site_Key IS NULL THEN 0
		     WHEN TGT.Site_Key IS NOT NULL AND MD5(NVL(TRIM(SRC.Site_Escalation_Type),'')||NVL(TRIM(SRC.Site_Access_Type),'')||SRC.Anchor_Customer_Key||NVL(TRIM(SRC.Tower_Type),'')||NVL(SRC.Tenancy_Count,0)||NVL(TRIM(SRC.Green_Site),'')||NVL(SRC.Green_Site_Date,'9999-12-31')) <> MD5(NVL(TRIM(TGT.Site_Escalation_Type),'')||NVL(TRIM(TGT.Site_Access_Type),'')||TGT.Anchor_Customer_Key||NVL(TRIM(TGT.Tower_Type),'')||NVL(TGT.Tenancy_Count,0)||NVL(TRIM(TGT.Green_Site),'')||NVL(TGT.Green_Site_Date,'9999-12-31')) THEN 2
		     WHEN TGT.Site_Key IS NOT NULL AND MD5(TRIM(SRC.Site_Name)||TRIM(SRC.Site_Type)||NVL(TRIM(SRC.Comfort_Factor),'')||TRIM(SRC.Feeder_Id)||NVL(SRC.Shelter_Length,0)||NVL(SRC.Shelter_Width,0)||NVL(SRC.Shelter_Height,0)||NVL(SRC.Shelter_Size,0)||NVL(SRC.Sanctioned_Load,0)||NVL(TRIM(SRC.Site_Equipment),'')||NVL(TRIM(SRC.Site_Location),'')||NVL(SRC.Approved_Extra_Storage,'F')||SRC.FCU_Only_Site||NVL(TRIM(SRC.Site_Network_Type),'')) <> MD5(TRIM(TGT.Site_Name)||TRIM(TGT.Site_Type)||NVL(TRIM(TGT.Comfort_Factor),'')||TRIM(TGT.Feeder_Id)||NVL(TGT.Shelter_Length,0)||NVL(TGT.Shelter_Width,0)||NVL(TGT.Shelter_Height,0)||NVL(TGT.Shelter_Size,0)||NVL(TGT.Sanctioned_Load,0)||NVL(TRIM(TGT.Site_Equipment),'')||NVL(TRIM(TGT.Site_Location),'')||NVL(TGT.Approved_Extra_Storage,'F')||TGT.FCU_Only_Site||NVL(TRIM(TGT.Site_Network_Type),'')) THEN 1
		ELSE 4 END
	      ) AS OPERATION_FLAG
	    , ( CASE WHEN TGT.Site_Key IS NULL THEN 'NEW RECORD - INSERT'
		     WHEN TGT.Site_Key IS NOT NULL AND MD5(NVL(TRIM(SRC.Site_Escalation_Type),'')||NVL(TRIM(SRC.Site_Access_Type),'')||SRC.Anchor_Customer_Key||NVL(TRIM(SRC.Tower_Type),'')||NVL(SRC.Tenancy_Count,0)||NVL(TRIM(SRC.Green_Site),'')||NVL(SRC.Green_Site_Date,'9999-12-31')) <> MD5(NVL(TRIM(TGT.Site_Escalation_Type),'')||NVL(TRIM(TGT.Site_Access_Type),'')||TGT.Anchor_Customer_Key||NVL(TRIM(TGT.Tower_Type),'')||NVL(TGT.Tenancy_Count,0)||NVL(TRIM(TGT.Green_Site),'')||NVL(TGT.Green_Site_Date,'9999-12-31')) THEN 'SCD TYPE-2'
		     WHEN TGT.Site_Key IS NOT NULL AND MD5(TRIM(SRC.Site_Name)||TRIM(SRC.Site_Type)||NVL(TRIM(SRC.Comfort_Factor),'')||TRIM(SRC.Feeder_Id)||NVL(SRC.Shelter_Length,0)||NVL(SRC.Shelter_Width,0)||NVL(SRC.Shelter_Height,0)||NVL(SRC.Shelter_Size,0)||NVL(SRC.Sanctioned_Load,0)||NVL(TRIM(SRC.Site_Equipment),'')||NVL(TRIM(SRC.Site_Location),'')||NVL(SRC.Approved_Extra_Storage,'F')||SRC.FCU_Only_Site||NVL(TRIM(SRC.Site_Network_Type),'')) <> MD5(TRIM(TGT.Site_Name)||TRIM(TGT.Site_Type)||NVL(TRIM(TGT.Comfort_Factor),'')||TRIM(TGT.Feeder_Id)||NVL(TGT.Shelter_Length,0)||NVL(TGT.Shelter_Width,0)||NVL(TGT.Shelter_Height,0)||NVL(TGT.Shelter_Size,0)||NVL(TGT.Sanctioned_Load,0)||NVL(TRIM(TGT.Site_Equipment),'')||NVL(TRIM(TGT.Site_Location),'')||NVL(TGT.Approved_Extra_Storage,'F')||TGT.FCU_Only_Site||NVL(TRIM(TGT.Site_Network_Type),'')) THEN 'SCD TYPE-1'
		ELSE 'NO CHANGE - IGNORE' END
	      ) AS OPERATION_TYPE
	FROM DEV_DIM.SITE_DIM_SOURCE_DATA_2 SRC
	LEFT OUTER JOIN DEV_DIM.SITE_DIMENSION TGT
	ON SRC.Indus_Site_ID = TGT.Indus_Site_ID
	AND TGT.Active_Flg = 'Y'"""

	DB.execute(SQL_STMT);

	# -- Step 4: Insert The Fresh Records
	SQL_STMT="""INSERT INTO DEV_DIM.SITE_DIMENSION
	(
	      Site_Key
	    , Indus_Site_ID
	    , Site_Name
	    , Site_Type
	    , Comfort_Factor
	    , Feeder_Id
	    , Shelter_Length
	    , Shelter_Width
	    , Shelter_Height
	    , Shelter_Size
	    , Sanctioned_Load
	    , Site_Equipment
	    , Site_Location
	    , Site_Escalation_Type
	    , Site_Access_Type
	    , Approved_Extra_Storage
	    , Anchor_Customer_Key
	    , Tower_Type
	    , Tenancy_Count
	    , Green_Site
	    , Green_Site_Date
	    , FCU_Only_Site
	    , Site_Network_Type
	    , Theft_Prone_Indicator
	    , Site_Tower_Type
	    , Eff_Start_Date
	    , Eff_End_Date
	    , ACTIVE_FLG
	)
	SELECT
	      dev_dim.Site_Dimension_Site_Key.nextval AS Site_Key
	    , Indus_Site_ID
	    , Site_Name
	    , Site_Type
	    , Comfort_Factor
	    , Feeder_Id
	    , Shelter_Length
	    , Shelter_Width
	    , Shelter_Height
	    , Shelter_Size
	    , Sanctioned_Load
	    , Site_Equipment
	    , Site_Location
	    , Site_Escalation_Type
	    , Site_Access_Type
	    , Approved_Extra_Storage
	    , Anchor_Customer_Key
	    , Tower_Type
	    , Tenancy_Count
	    , Green_Site
	    , Green_Site_Date
	    , FCU_Only_Site
	    , Site_Network_Type
	    , Theft_Prone_Indicator
	    , Site_Tower_Type
	    , create_date AS Eff_Start_Date
	    , '9999-12-31'
	    , 'Y'
	FROM DEV_DIM.SITE_DIM_SOURCE_DATA_2 A
	WHERE A.Indus_Site_ID IN
	(   SELECT Indus_Site_ID FROM DEV_DIM.SITE_TEMP_XREF
	    WHERE OPERATION_FLAG = 0
	);"""

	DB.execute(SQL_STMT);


	# -- Step 5: SCD Type-1 Update the records with New Type-1 values
	SQL_STMT="""UPDATE DEV_DIM.SITE_DIMENSION TGT
	SET   Site_Name = SRC.Site_Name
	    , Site_Type = SRC.Site_Type
	    , Comfort_Factor = SRC.Comfort_Factor
	    , Feeder_Id = SRC.Feeder_Id
	    , Shelter_Length = SRC.Shelter_Length
	    , Shelter_Width = SRC.Shelter_Width
	    , Shelter_Height = SRC.Shelter_Height
	    , Shelter_Size = SRC.Shelter_Size
	    , Sanctioned_Load = SRC.Sanctioned_Load
	    , Site_Equipment = SRC.Site_Equipment
	    , Site_Location = SRC.Site_Location
	    , Approved_Extra_Storage = SRC.Approved_Extra_Storage
	    , FCU_Only_Site = SRC.FCU_Only_Site
	    , Site_Network_Type = SRC.Site_Network_Type
	FROM DEV_DIM.SITE_DIM_SOURCE_DATA_2 SRC
	WHERE TGT.Indus_Site_ID = SRC.Indus_Site_ID
	AND TGT.Active_Flg = 'Y'
	AND TGT.Indus_Site_ID IN
	(   SELECT Indus_Site_ID FROM DEV_DIM.SITE_TEMP_XREF
	    WHERE OPERATION_FLAG = 1
	);"""

	DB.execute(SQL_STMT);

	# -- Step 6: SCD Type-2 Update the old records to be Inactive
	SQL_STMT="""UPDATE DEV_DIM.SITE_DIMENSION TGT
	SET Active_Flg = 'N', Eff_End_Date = DATE(SRC.Create_Date)
	FROM DEV_DIM.SITE_DIM_SOURCE_DATA_2 SRC
	WHERE TGT.Indus_Site_ID = SRC.Indus_Site_ID
	AND TGT.Active_Flg = 'Y'
	AND SRC.Indus_Site_ID IN
	(   SELECT Indus_Site_ID FROM DEV_DIM.SITE_TEMP_XREF
	    WHERE OPERATION_FLAG = 2
	);"""

	DB.execute(SQL_STMT);

	# -- Step 7: SCD Type-2 Insert the records with updated Type-2 values
	SQL_STMT="""INSERT INTO DEV_DIM.SITE_DIMENSION
	(
	      Site_Key
	    , Indus_Site_ID
	    , Site_Name
	    , Site_Type
	    , Comfort_Factor
	    , Feeder_Id
	    , Shelter_Length
	    , Shelter_Width
	    , Shelter_Height
	    , Shelter_Size
	    , Sanctioned_Load
	    , Site_Equipment
	    , Site_Location
	    , Site_Escalation_Type
	    , Site_Access_Type
	    , Approved_Extra_Storage
	    , Anchor_Customer_Key
	    , Tower_Type
	    , Tenancy_Count
	    , Green_Site
	    , Green_Site_Date
	    , FCU_Only_Site
	    , Site_Network_Type
	    , Theft_Prone_Indicator
	    , Site_Tower_Type
	    , Eff_Start_Date
	    , Eff_End_Date
	    , ACTIVE_FLG
	)
	SELECT
	      dev_dim.Site_Dimension_Site_Key.nextval AS Site_Key
	    , Indus_Site_ID
	    , Site_Name
	    , Site_Type
	    , Comfort_Factor
	    , Feeder_Id
	    , Shelter_Length
	    , Shelter_Width
	    , Shelter_Height
	    , Shelter_Size
	    , Sanctioned_Load
	    , Site_Equipment
	    , Site_Location
	    , Site_Escalation_Type
	    , Site_Access_Type
	    , Approved_Extra_Storage
	    , Anchor_Customer_Key
	    , Tower_Type
	    , Tenancy_Count
	    , Green_Site
	    , Green_Site_Date
	    , FCU_Only_Site
	    , Site_Network_Type
	    , Theft_Prone_Indicator
	    , Site_Tower_Type
	    , create_date AS Eff_Start_Date
	    , '9999-12-31'
	    , 'Y'
	FROM DEV_DIM.SITE_DIM_SOURCE_DATA_2 A
	WHERE A.Indus_Site_ID IN
	(   SELECT Indus_Site_ID FROM DEV_DIM.SITE_TEMP_XREF
	    WHERE OPERATION_FLAG = 2
	);"""

	DB.execute(SQL_STMT);

	RANK_COUNTER_VARIABLE = RANK_COUNTER_VARIABLE + 1
