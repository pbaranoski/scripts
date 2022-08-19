
WITH PSPS_DTL_INFO as (

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

	   ,SUM(A.CLM_LINE_SBMT_SRVC_UNIT_QTY)   AS SBMT_SRVC_UNIT_QTY                                                     
	   ,SUM(A.CLM_LINE_SBMT_CHRG_AMT)        AS SBMT_CHRG_AMT                                                     
	   ,SUM(A.CLM_LINE_ALOWD_CHRG_AMT)       AS ALOWD_CHRG_AMT                                                     
	   ,SUM(A.CLM_LINE_DND_SRVC_UNIT_QTY)    AS DND_SRVC_UNIT_QTY 
	   ,SUM(A.CLM_LINE_DND_AMT)              AS DND_AMT 
	   ,SUM(A.CLM_LINE_ASGND_SRVC_UNIT_QTY)  AS ASGND_SRVC_UNIT_QTY
	   ,to_char(SUM(A.CLM_LINE_CVRD_PD_AMT),'S00000000000.00')  AS CVRD_PD_AMT
								
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
								
	FROM BIA_DEV.CMS_AGG_PTB_DEV.CLM_PTB_MO_AGG A  
									  
	INNER JOIN BIA_DEV.CMS_DIM_PTB_DEV.CLM_CYQ_SGNTR B                      
	ON    A.CLM_CYQ_SGNTR_SK = B.CLM_CYQ_SGNTR_SK      
						 
	LEFT OUTER JOIN IDRC_DEV.CMS_DIM_CLNDR_DEV.CLNDR_CYQ SERV_CYQ              
	ON    B.CLNDR_SRVC_CYQ_SK = SERV_CYQ.CLNDR_CYQ_SK   
						
	LEFT OUTER JOIN IDRC_DEV.CMS_DIM_CLNDR_DEV.CLNDR_CYQ PROC_CYQ              
	ON    B.CLNDR_PROC_CYQ_SK = PROC_CYQ.CLNDR_CYQ_SK 
						 
	LEFT OUTER JOIN IDRC_DEV.CMS_DIM_CLNDR_DEV.CLNDR_CYQ SP_CYQ                
	ON    B.CLNDR_SCHLD_PMT_CYQ_SK = SP_CYQ.CLNDR_CYQ_SK  
						 
	INNER JOIN BIA_DEV.CMS_DIM_PTB_DEV.CLM_YEAR_SGNTR D                        
	ON    B.CLM_YEAR_SGNTR_SK = D.CLM_YEAR_SGNTR_SK   
							 
	INNER JOIN BIA_DEV.CMS_DIM_PTB_DEV.CLM_CD_SGNTR E                          
	ON    A.CLM_CD_SGNTR_SK = E.CLM_CD_SGNTR_SK      
							
	LEFT OUTER JOIN IDRC_DEV.CMS_DIM_HCPCS_DEV.HCPCS_CD H                         
	ON    A.CLM_LINE_HCPCS_CD = H.HCPCS_CD                                     
	AND    A.CLNDR_HCPCS_YR_NUM = H.CLNDR_HCPCS_YR_NUM                         
																				   
	WHERE SERV_CYQ.CLNDR_CYQ_NAME BETWEEN 'CY2019Q1' AND 'CY2019Q4'                                                                    
	AND   PROC_CYQ.CLNDR_CYQ_NAME BETWEEN 'CY2019Q1' AND 'CY2019Q4'                                                                    

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
			,A.CLM_LINE_INVLD_HCPCS_CD

)

SELECT 
       HCPCS_CD 
      ,INTI_MOD    
	  ,SPECIALTY_CD    
	  ,CARRIER_NUM   
	  ,LOCALITY_CD 
	  ,TOS  
	  ,POS
	  ,SECOND_MOD 
	  ,CASE WHEN SBMT_SRVC_UNIT_QTY < 11
			THEN RPAD('*',14,' ')
			ELSE to_char(SBMT_SRVC_UNIT_QTY,'FM0000000000.000')
	    END  as  SBMT_SRVC_UNIT_QTY		
	  ,CASE WHEN SBMT_SRVC_UNIT_QTY < 11
	        THEN RPAD('*',15,' ')
			ELSE to_char(SBMT_CHRG_AMT,'S00000000000.00')
		END	as SBMT_CHRG_AMT
	  ,CASE WHEN ASGND_SRVC_UNIT_QTY < 11 
	        THEN RPAD('*',15,' ')
			ELSE to_char(ALOWD_CHRG_AMT,'S00000000000.00')  
		END as ALOWD_CHRG_AMT 	 
	  ,CASE WHEN DND_SRVC_UNIT_QTY < 11
            THEN RPAD('*',14,' ')
            ELSE to_char(DND_SRVC_UNIT_QTY,'FM0000000000.000') 
	   END as DND_SRVC_UNIT_QTY 		 
	  ,CASE WHEN DND_SRVC_UNIT_QTY < 11
	        THEN RPAD('*',15,' ')
			ELSE to_char(DND_AMT,'S00000000000.00')
	   END as DND_AMT	
	  ,CASE WHEN ASGND_SRVC_UNIT_QTY < 11
	        THEN RPAD('*',14,' ')
            ELSE to_char(ASGND_SRVC_UNIT_QTY,'FM0000000000.000') 
       END as ASGND_SRVC_UNIT_QTY			
	  ,CVRD_PD_AMT
	  ,HCPCS_ASC_IND_CD 
	  ,CLM_ERR_SGNTR_SK 
	  ,BETOS              
		 
FROM PSPS_DTL_INFO	
		
;