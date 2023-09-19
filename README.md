# KNiNe
Implementation of Hashing algorithms for building graphs and ML Graph algorithms in Spark

Compilation:

    cd <PATH_TO_KNINE>

    sbt clean assembly

Execution:

    spark-submit [--master "local[NUM_THREADS]"] --class es.udc.graph.KNiNe <PATH_TO_JAR_FILE> <INPUT_DATASET> <OUTPUT_GRAPH> [options]

Usage: KNiNe dataset output_file [options]

Dataset must be a libsvm or text file

Options:

    -k    Number of neighbors (default: 10)
    
    -m    Method used to compute the graph. Valid values: vrlsh, brute, fastKNN-proj, fastKNN-AGH (default: vrlsh)
    
    -r    Starting radius (default: 0.1)
    
    -t    Maximum comparisons per item (default: auto)
    
    -c    File containing the graph to compare to (default: nothing)
    
    -p    Number of partitions for the data RDDs (default: 512)
    
    -d    Number of refinement (descent) steps (LSH only) (default: 1)
    
    -b    blockSz (fastKNN only) (default: 100)
    
    -i    iterations (fastKNN only) (default: 1)

## Reference

Eiras-Franco, C., Martínez-Rego, D., Kanthan, L., Piñeiro, C., Bahamonde, A., Guijarro-Berdiñas, B., & Alonso-Betanzos, A. (2020). Fast Distributed k NN Graph Construction Using Auto-tuned Locality-sensitive Hashing. ACM Transactions on Intelligent Systems and Technology (TIST), 11(6), 1-18.
[https://doi.org/10.1145/3408889](https://doi.org/10.1145/3408889)