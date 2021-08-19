import csv




#This is just sample Op 
class Uploading_data_to_db():
    def __init__(self,config_file,db_config_file):     
        with open(config_file) as cred:
            cred_data            = json.load(cred)
            self.dataSource      = cred_data["dataSource"]
            self.baseDir         = cred_data["baseDir"]
            self.chunksize       = cred_data["chunksize"]
            self.batchSize       = cred_data["batchSize"]
            self.tempTable       = cred_data["tempTable"]
            self.fileTable       = cred_data["fileTable"]
            self.schema          = cred_data["schema"]
            self.csvDir          = cred_data["csvDir"]

        with open(db_config_file) as dbcred:
            db_cred_data            = json.load(dbcred)
            self.driver      = db_cred_data["driver"]
            self.server         = db_cred_data["server"]
            self.database       = db_cred_data["database"]
            self.username       = db_cred_data["username"]
            self.password       = db_cred_data["password"]


	def check_files_availability(self,config_file):

		csv_files = glob.glob(os.path.join(self.csvDir,'*.csv'))
		csv_files_count=len(csv_files)
		if csv_files_count > 0 :
		    csv_files.sort(key=os.path.getmtime)
		    csv_files_count = csv_files_count if int(self.batchSize) > csv_files_count else int(self.batchSize)
		    
		    return csv_files
		else:
		    return False,0
    def db_conn(self,db_config_file):
        params = 'DRIVER=' + self.driver + ';SERVER='+self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password
        db_params = urllib.parse.quote_plus(params)
        try:
            engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))
            conn = engine.connect()
            return conn,True
        except Exception as err:
            return err,False
	def upload_real(self,db_config_file,config_file,df,temp_table,schema,engine,chunksize):

		@event.listens_for(engine, "before_cursor_execute")
		def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
		    if executemany:
		        cursor.fast_executemany = True
		df.to_csv('hh.csv')
		file_row_count = df.shape[0]

		engine,err = self.db_conn(db_config_file)
	
	def looping_csv_files(self,config_file,db_config_file):
	    csv_files = self.check_files_availability(config_file)

	    for full_name in csv_files:
	        #df = pd.read_csv(full_name,sep='\t',low_memory=False)
	        df = pd.read_csv(full_name,sep='|')
	        #print(full_name)

	 
	        try:
	            qry = "INSERT INTO " + "TEST" + ".CUSTOMER_FILES(Customer_Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,DOB,Is_Active) VALUES (?,?,?,?,?,?,?,?,?)"
	           
	            engine.execute(qry)
	            #consider that fileid to add to dataframe fileid column
	           
	            #Upload to DB
	            load_flag,stat = self.upload_real(db_config_file,config_file,df,temp_table,schema,engine)

	            if load_flag:
	                #print('Uploaded to DB')
	                print('Uploaded to CUSTOMER_FILES Table')
	                #print('Successfully loading to DB')
	    

	            else:
	                print("Error while loading data to CUSTOMER_FILES table")
	                #print('Files are moving from FTP to Error folder')
	                continue
	            #process_logger.info("Uploaded: {} rows out of {} rows".format(df.index[-1]+1,file_row_count))
	        except:
	           print('Error while uploading')