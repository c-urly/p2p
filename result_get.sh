#!/bin/bash

# Output CSV file
output_file="results.csv"

# Fixed number of requests
num_requests=8

# Create CSV file with header
echo "num_nodes,average_hops" > "$output_file"

# Loop from 10 to 1024 with an interval of 10
for num_nodes in $(seq 10 10 1024); do
    echo "Running for $num_nodes nodes..."
    
    # Run the command and capture the output
    output=$(./p2p $num_nodes $num_requests)
    
    # Extract the average number of hops
    average_hops=$(echo "$output" | grep "Average number of hops per lookup:" | awk '{print $7}')
    
    # Print the message
    echo "Average number of hops per lookup: $average_hops"
    
    # Append to CSV file
    echo "$num_nodes,$average_hops" >> "$output_file"
done

echo "Script completed. Results saved in $output_file"