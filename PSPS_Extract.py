#!/usr/bin/env python
########################################################################################################
# Name:  PSPS_Extract.py
#
# Desc: Script to Extract PSPS data (IDR#PSPSQ1-Q6)
#
# Created: Viren Khanna 
# Modified: 07/25/2022
# Modified: 8/4/2022 - Paul Baranoski - remove "Compression = None" parameter.
########################################################################################################
import os
import sys
import datetime
from datetime import datetime
import sendEmail

currentDirectory = os.path.dirname(os.path.realpath(__file__))
rootDirectory = os.path.abspath(os.path.join(currentDirectory, ".."))
utilDirectory = os.getenv('CMN_UTIL')

sys.path.append(rootDirectory)
sys.path.append(utilDirectory)
script_name = os.path.basename(__file__)

import snowconvert_helpers
from snowconvert_helpers import Export

########################################################################################################
# VARIABLE ASSIGNMENT
########################################################################################################
con = None 
now = datetime.now()
date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

TMSTMP=os.getenv('TMSTMP')
ENVNAME=os.getenv('ENVNAME')
CLNDR_CYQ_BEG_DT=os.getenv('CLNDR_CYQ_BEG_DT')
CLNDR_CYQ_END_DT=os.getenv('CLNDR_CYQ_END_DT')
QTR=os.getenv('QTR')

# set email variables
sender=os.getenv('PSPS_EMAIL_SENDER')
success_receivers=os.getenv('PSPS_EMAIL_SUCCESS_RECIPIENT') 

# boolean - Python Exception status
bPythonExceptionOccurred=False

########################################################################################################
# RUN
########################################################################################################
print('')
print("Run date and time: " + date_time  )
print('')

########################################################################################################
# Method to execute the extract SQL using Timestamp 
########################################################################################################
try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()   
   snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)
   
   #**************************************
   #   Extract Part D claim data  
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_STG/PSPS_Extract_{QTR}_{TMSTMP}.csv.gz
                                                FROM (

                                 
                                 SELECT        
		RPAD(CASE                                                                          
			 WHEN A.CLM_LINE_INVLD_HCPCS_CD = '~' 
			 THEN COALESCE(H.HCPCS_CD,' ')                       
			 ELSE A.CLM_LINE_INVLD_HCPCS_CD                                            
			 END,5,' ')  AS HCPCS_CD 

	   ,RPAD(CASE 
			 WHEN E.HCPCS_1_MDFR_CD IS NULL 
			   OR E.HCPCS_1_MDFR_CD = '~' 
			 THEN ' '  
			 ELSE E.HCPCS_1_MDFR_CD 
			 END,2,' ')  AS INTI_MOD    
	   ,RPAD(CASE 
			 WHEN E.CLM_RNDRG_FED_PRVDR_SPCLTY_CD IS NULL 
			   OR E.CLM_RNDRG_FED_PRVDR_SPCLTY_CD ='~' 
			 THEN ' ' 
			 ELSE E.CLM_RNDRG_FED_PRVDR_SPCLTY_CD 
			 END,2,' ')  AS SPECIALTY_CD    
	   ,RPAD(CASE 
			 WHEN A.CLM_CNTRCTR_NUM ='~' 
			 THEN ' '  
			 ELSE A.CLM_CNTRCTR_NUM 
			 END,5,' ') AS CARRIER_NUM   
			 
	   ,RPAD(CASE 
			 WHEN E.CLM_PRCNG_LCLTY_CD ='~' 
			 THEN ' '   
			 ELSE E.CLM_PRCNG_LCLTY_CD 
			 END,2,' ')  AS LOCALITY_CD 
	   ,RPAD(CASE 
			 WHEN E.CLM_FED_TYPE_SRVC_CD ='~' 
			 THEN ' '   
			 ELSE E.CLM_FED_TYPE_SRVC_CD 
			 END,1,' ') AS TOS  
	   ,RPAD(CASE 
			 WHEN E.CLM_POS_CD ='~' 
			 THEN ' '   
			 ELSE E.CLM_POS_CD 
			 END,2,' ') AS POS
	   ,RPAD(CASE 
			 WHEN E.HCPCS_2_MDFR_CD is NULL
			   OR E.HCPCS_2_MDFR_CD ='~' 
			 THEN ' ' 
			 ELSE E.HCPCS_2_MDFR_CD 
			 END,2,' ') AS SECOND_MOD 

	   ,to_char(SUM(A.CLM_LINE_SBMT_SRVC_UNIT_QTY),'FM0000000000.000')  AS M8                                                     
	   ,to_char(SUM(A.CLM_LINE_SBMT_CHRG_AMT),'S00000000000.00')        AS M1                                                     
	   ,to_char(SUM(A.CLM_LINE_ALOWD_CHRG_AMT),'S00000000000.00')       AS M2                                                     
	   ,to_char(SUM(A.CLM_LINE_DND_SRVC_UNIT_QTY),'FM0000000000.000')     AS M6 
	   ,to_char(SUM(A.CLM_LINE_DND_AMT),'S00000000000.00')              AS M5 
	   ,to_char(SUM(A.CLM_LINE_ASGND_SRVC_UNIT_QTY),'FM0000000000.000') AS M4
	   ,to_char(SUM(A.CLM_LINE_CVRD_PD_AMT),'S00000000000.00')          AS M3
								
	   ,(CASE 
		 WHEN H.HCPCS_ASC_IND_CD IS NULL
		   OR H.HCPCS_ASC_IND_CD = '~' 
		 THEN ' '  
		 ELSE H.HCPCS_ASC_IND_CD       
		 END)  AS HCPCS_ASC_IND_CD 
		 
	   ,to_char(A.CLM_ERR_SGNTR_SK,'FM00') AS CLM_ERR_SGNTR_SK 
	   
	   ,RPAD(CASE 
		 WHEN H.HCPCS_BETOS_CD IS NULL 
		   OR H.HCPCS_BETOS_CD = '~' 
		 THEN ' '   
		 ELSE H.HCPCS_BETOS_CD 
		 END,3,' ') AS BETOS                          
								
	FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_PTB_MO_AGG A  
									  
	INNER JOIN BIA_{ENVNAME}.CMS_DIM_PTB_{ENVNAME}.CLM_CYQ_SGNTR B                      
	ON    A.CLM_CYQ_SGNTR_SK = B.CLM_CYQ_SGNTR_SK      
						 
	LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_CLNDR_{ENVNAME}.CLNDR_CYQ SERV_CYQ              
	ON    B.CLNDR_SRVC_CYQ_SK = SERV_CYQ.CLNDR_CYQ_SK   
						
	LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_CLNDR_{ENVNAME}.CLNDR_CYQ PROC_CYQ              
	ON    B.CLNDR_PROC_CYQ_SK = PROC_CYQ.CLNDR_CYQ_SK 
						 
	LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_CLNDR_{ENVNAME}.CLNDR_CYQ SP_CYQ                
	ON    B.CLNDR_SCHLD_PMT_CYQ_SK = SP_CYQ.CLNDR_CYQ_SK  
						 
	INNER JOIN BIA_{ENVNAME}.CMS_DIM_PTB_{ENVNAME}.CLM_YEAR_SGNTR D                        
	ON    B.CLM_YEAR_SGNTR_SK = D.CLM_YEAR_SGNTR_SK   
							 
	INNER JOIN BIA_{ENVNAME}.CMS_DIM_PTB_{ENVNAME}.CLM_CD_SGNTR E                          
	ON    A.CLM_CD_SGNTR_SK = E.CLM_CD_SGNTR_SK      
							
	LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_HCPCS_{ENVNAME}.HCPCS_CD H                         
	ON    A.CLM_LINE_HCPCS_CD = H.HCPCS_CD                                     
	AND    A.CLNDR_HCPCS_YR_NUM = H.CLNDR_HCPCS_YR_NUM                         
																				   
	WHERE SERV_CYQ.CLNDR_CYQ_NAME BETWEEN '{CLNDR_CYQ_BEG_DT}' AND '{CLNDR_CYQ_END_DT}'                                                                    
	AND   PROC_CYQ.CLNDR_CYQ_NAME BETWEEN '{CLNDR_CYQ_BEG_DT}' AND '{CLNDR_CYQ_END_DT}'                                                                    

	GROUP BY                                                                     
			 A.CLM_ERR_SGNTR_SK                                                             
			,A.CLM_CNTRCTR_NUM                                                              
			,E.CLM_PRCNG_LCLTY_CD                                                           
			,E.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                                
			,E.CLM_FED_TYPE_SRVC_CD                                                         
			,E.CLM_POS_CD                                                                   
			,E.HCPCS_1_MDFR_CD                                                              
			,E.HCPCS_2_MDFR_CD                                                              
			,H.HCPCS_CD                                                                     
			,H.HCPCS_BETOS_CD                                                               
			,H.HCPCS_ASC_IND_CD                                                             
			,A.CLM_LINE_INVLD_HCPCS_CD )
                        FILE_FORMAT = (TYPE = CSV field_delimiter = none FIELD_OPTIONALLY_ENCLOSED_BY = none )
                        SINGLE = TRUE  max_file_size=5368709120  """, con, exit_on_error=True)



   #**************************************
   #   Send Email of Success  
   #**************************************
   print("before sendEmail")  

   subject = "PSPS Quarterly Extract"
   message = f"""PSPS Quarterly Extract has completed. 
   
                 The following file was created: PSPS_Extract_{QTR}_{TMSTMP}.csv.gz """


   sendEmail.sendEmail(sender, success_receivers, subject, message)
   
   
   #**************************************
   # End Application
   #**************************************    
   snowconvert_helpers.quit_application()
   
except Exception as e:
   print(e)
   
   # Let shell script know that python code failed.
   bPythonExceptionOccurred=True   
   
finally:
   if con is not None:
      con.close()

   # Let shell script know that python code failed.      
   if bPythonExceptionOccurred == True:
      sys.exit(12) 
   else:   
      snowconvert_helpers.quit_application()
