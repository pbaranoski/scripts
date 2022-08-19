#!/usr/bin/bash
#
######################################################################################
# Name:  PSPS_Extract.sh
#
# Desc: PSPS Extract for Q1 thru Q6 extracts. 
#
# Created: Viren Khanna  07/15/2022
# Modified: 2022-08-04 - Paul Baranoski - Added "else" statement to send message to log that
#                                       Extract is not schedule to run in non-Qtr months.
#           2022-08-16 - Paul Baranoski - Added code to unzip .gz Q6 file (needed by Split files script)
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PSPS_Extract_Val_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/



touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PSPS_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

######################################################################################
# PSPS File Schedule Example
# For demonstration purposes, the 8th of the month following the end of the 
#     quarter will be used as the day on which the PSPS files are run.
#
# 4/08/2020
# PSPS 2020Q1 is generated. This file will contain claims incurred in Q1 2020 and processed on 1/1/2020 through 3/31/2020
#
# 7/08/2020
# PSPS 2020Q2 is generated. This file will contain claims incurred in Q1 and Q2 2020, and processed on 1/1/2020 through 6/30/2020
#
# 10/08/2020
# PSPS 2020Q3 is generated. This file will contain claims incurred in Q1, Q2, and Q3 2020, and processed on 1/1/2020 through 9/30/2020
#
# 1/08/2021
# PSPS 2020Q4 is generated. This file will contain claims incurred in Q1, Q2, Q3 and Q4 2020, and processed on 1/1/2020 through 12/31/2020
#
# 4/08/2021
# PSPS 2020Q5 is generated. This file will contain claims incurred in 2020, and processed on 1/1/2020 through 3/31/2021
# PSPS 2021Q1 is generated. This file will contain claims incurred in Q1 2021 and processed on 1/1/2021 through 3/31/2021
#
# 7/08/2021
# PSPS 2020Q6 is generated. This file will contain claims incurred in 2020, and processed on 1/1/2020 through 6/30/2021. 
#      This represents 6 quarters of 2020.
# PSPS 2021Q2 is generated. This file will contain claims incurred in Q1 and Q2 2021, and processed on 1/1/2021 through 6/30/2021
#
# 10/08/2021
# PSPS 2021Q3 is generated. This file will contain claims incurred in Q1, Q2, and Q3 2021, and processed on 1/1/2021 through 9/30/2021
######################################################################################

############################################
# Extract current and prior year
############################################
CUR_YR=`date +%Y`
PRIOR_YR=`expr ${CUR_YR} - 1` 

echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}
echo "PRIOR_YR=${PRIOR_YR}" >> ${LOGNAME}

############################################
# Determine Processing Qtr
############################################
MM=`date +%m`
MM="07"
if [ $MM = "04" ]; then
	QTR=Q1
elif [ $MM = "07" ]; then
	QTR=Q2
elif [ $MM = "10" ]; then
	QTR=Q3	
elif [ $MM = "01" ]; then
	QTR=Q4	
else
	echo "Extract is processed quarterly for months April, July, October, and January. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}
	
	# Send Did not run email	
	SUBJECT="PSPS Extract did not run."
	MSG="Extract is processed quarterly for months April, July, October, and January. Extract is not scheduled to run for this time period. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 0 	
fi

echo "QTR=${QTR}" >> ${LOGNAME}


############################################
# Build parms for appropriate Qtr
############################################
if [ $QTR == "Q1" ]; then
	CLNDR_CYQ_BEG_DT="CY${CUR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q1"
elif [ $QTR = "Q2" ]; then	
	CLNDR_CYQ_BEG_DT="CY${CUR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q2"
elif [ $QTR = "Q3" ]; then	
	CLNDR_CYQ_BEG_DT="CY${CUR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q3"
elif [ $QTR = "Q4" ]; then	
	CLNDR_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${PRIOR_YR}Q4"
fi	

echo "CLNDR_CYQ_BEG_DT=${CLNDR_CYQ_BEG_DT}" >> ${LOGNAME}
echo "CLNDR_CYQ_END_DT=${CLNDR_CYQ_END_DT}" >> ${LOGNAME}
	
export CLNDR_CYQ_BEG_DT
export CLNDR_CYQ_END_DT
export QTR
export TMSTMP

############################################
# Execute appropriate Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for appropriate Qtr between Q1-Q4. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PSPS_Extract.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PSPS_Extract.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS Extract (Q1-Q4) - Failed"
		MSG="PSPS extract (Q1-Q4) has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#################################################################################
# Copy Q4 (EARLY cut file to Linux as .txt file for use in PSPS_Split_files.bash
#################################################################################
if [ $QTR == "Q4" ]; then
	echo "" >> ${LOGNAME}
	echo "Starting copy of S3 Q4 file to Linux." >> ${LOGNAME}
			
	S3Q4Filename=PSPS_Extract_${QTR}_${TMSTMP}.csv.gz
	aws s3 cp s3://${bucket}${S3Q4Filename} ${DATADIR}PBAR_PSPS${QTR}_${TMSTMP}.txt.gz  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying Q4 S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PSPS Extract - Failed"
			MSG="PSPS Extract copy Q4 S3 file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
	
	##############################
	# gunzip Q6 file from S3
	##############################
	echo " " >> ${LOGNAME}
	echo "Unzip .gz Q4 file" >> ${LOGNAME}
	echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

	gzip -d ${DATADIR}PBAR_PSPS${QTR}_${TMSTMP}.txt.gz  2>>  ${LOGNAME}
fi	


############################################
# Build parms for Q5/6 if necessary
############################################
if [ $QTR == "Q1" ]; then
	CLNDR_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q1"
    QTR="Q5" 
elif [ $QTR == "Q2" ]; then	
	CLNDR_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q2"
    QTR="Q6"
else
	echo "Processing completed." >> ${LOGNAME}
	exit 0 	
fi

############################################
# Perform Qtr 5/6 processing 
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for appropriate Qtr between Q5-Q6. " >> ${LOGNAME}

echo "CLNDR_CYQ_BEG_DT=${CLNDR_CYQ_BEG_DT}" >> ${LOGNAME}
echo "CLNDR_CYQ_END_DT=${CLNDR_CYQ_END_DT}" >> ${LOGNAME}
	
export CLNDR_CYQ_BEG_DT
export CLNDR_CYQ_END_DT
export QTR
export TMSTMP

############################################
# Execute appropriate Qtr 5 or 6 Extract
############################################
${PYTHON_COMMAND} ${RUNDIR}PSPS_Extract.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PSPS_Extract.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS Extract (Q5-Q6) - Failed"
		MSG="PSPS extract (Q5-Q6) has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PSPS_Extract.py completed successfully. " >> ${LOGNAME}


######################################################################
# Copy Q6 file to Linux as .txt file for use in PSPS_Split_files.bash
######################################################################
if [ $QTR == "Q6" ]; then
	echo "" >> ${LOGNAME}
	echo "Starting copy of S3 Q6 file to Linux." >> ${LOGNAME}
			
	S3Q6Filename=PSPS_Extract_${QTR}_${TMSTMP}.csv.gz
	aws s3 cp s3://${bucket}${S3Q6Filename} ${DATADIR}PBAR_PSPS${QTR}_${TMSTMP}.txt.gz  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying Q6 S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PSPS Extract - Failed"
			MSG="PSPS Extract copy Q6 S3 file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
	
	##############################
	# gunzip Q6 file from S3
	##############################
	echo " " >> ${LOGNAME}
	echo "Unzip .gz Q6 file" >> ${LOGNAME}
	echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

	gzip -d ${DATADIR}PBAR_PSPS${QTR}_${TMSTMP}.txt.gz  2>>  ${LOGNAME}
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PSPS_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
