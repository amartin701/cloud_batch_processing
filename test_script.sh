FRONTEND_URL="http://localhost:5000"
NUM_JOBS=5
INTERVAL=1800
ANALYSIS_TYPE="qc"
BED_FILE=""
BIM_FILE=""
FAM_FILE=""

if [ -n "$1" ]; then NUM_JOBS=$1; fi
if [ -n "$2" ]; then INTERVAL=$2; fi
if [ -n "$3" ]; then ANALYSIS_TYPE=$3; fi
if [ -n "$4" ]; then BED_FILE=$4; fi
if [ -n "$5" ]; then BIM_FILE=$5; fi
if [ -n "$6" ]; then FAM_FILE=$6; fi

echo "Submitting $NUM_JOBS jobs every ${INTERVAL}s (type: $ANALYSIS_TYPE)"

for i in $(seq 1 $NUM_JOBS); do
    echo "Submitting job $i..."

    payload="{\"analysis_type\": \"$ANALYSIS_TYPE\", \"job_id\": \"test_job_$i\", \"bed_file\": \"$BED_FILE\", \"bim_file\": \"$BIM_FILE\", \"fam_file\": \"$FAM_FILE\"}"
    
    curl -s -X POST "$FRONTEND_URL/trigger-job" \
        -H "Content-Type: application/json" \
        -d "$payload"
    
    echo ""
    
    if [ $i -lt $NUM_JOBS ]; then
        sleep $INTERVAL
    fi
done

echo "Done! Submitted $NUM_JOBS jobs."