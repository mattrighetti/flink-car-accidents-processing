
OUTPUT_FILE="/Users/luchetto/benchmarks/benchmark"
OUTPUT_FILE_SMALL="/Users/luchetto/benchmarks/benchmark_small"

MAX_PARALLELISM=3
QUERY_NUM=7

for ((i = 1 ; i <= ${QUERY_NUM} ; i++)); do
    for ((j = 1 ; j <= ${MAX_PARALLELISM} ; j++)); do
        echo "query:"${i} >> ${OUTPUT_FILE_SMALL}
        echo "parallelism:"${j} >> ${OUTPUT_FILE_SMALL}
        ./flink-1.11.1/bin/flink \
        run -p ${j} -c it.polimi.middleware.flink.tutorial.batch.accidents.CarAccidentsBenchmark \
        flink-car-accidents-processing/target/tutorial-1.jar \
        --nypd_data_file /opt/flink/NYPD_Motor_Vehicle_Collisions_small.csv \
        --query ${i} \
        --output /opt/flink/output${i} >> ${OUTPUT_FILE_SMALL}
    done
done

for ((i = 1 ; i <= ${QUERY_NUM} ; i++)); do
    for ((j = 1 ; j <= ${MAX_PARALLELISM} ; j++)); do
        echo "query:"${i} >> ${OUTPUT_FILE}
        echo "parallelism:"${j} >> ${OUTPUT_FILE}
        ./flink-1.11.1/bin/flink \
        run -p ${j} -c it.polimi.middleware.flink.tutorial.batch.accidents.CarAccidentsBenchmark \
        flink-car-accidents-processing/target/tutorial-1.jar \
        --nypd_data_file /opt/flink/NYPD_Motor_Vehicle_Collisions.csv \
        --query ${i} \
        --output /opt/flink/output${i} >> ${OUTPUT_FILE}
    done
done