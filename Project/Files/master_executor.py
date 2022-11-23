import Download


# Path = "D:\\Ingestion_Kart\\Pipeline\\Project\\P_555\\Task\\"
Path="D:\\test\\test\\"
Project_id ='P_555'
Task_id ='1234567'


Download.download_engine(Project_id,Task_id,Path)
Download.download_json(Project_id,Task_id,Path)
Download.execute_engine(Project_id,Task_id,Path)



    
    



