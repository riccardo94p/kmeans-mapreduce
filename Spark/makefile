.PHONY: all clean

all:
	spark-submit k-means.py --input "Resources/Input/points_100000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 5 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 9 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 11 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 13 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_1000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_10000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_1000000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_10000000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x3.txt" --output "Resources/Output/Spark_param_test" --dimension 3 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x4.txt" --output "Resources/Output/Spark_param_test" --dimension 4 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x5.txt" --output "Resources/Output/Spark_param_test" --dimension 5 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x6.txt" --output "Resources/Output/Spark_param_test" --dimension 6 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
	spark-submit k-means.py --input "Resources/Input/points_100000x7.txt" --output "Resources/Output/Spark_param_test" --dimension 7 --num_k 7 --iterations 10 --threshold 0.03 --seed 2 --master yarn
	stop-dfs.sh && stop-yarn.sh
	start-dfs.sh && start-yarn.sh
