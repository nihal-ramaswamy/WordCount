# MapReduce
## About
Simple implementation of mapreduce in golang. 

## Set Up
This project has two folders `master` and `worker`. Each of these are separate golang modules.

## Running 
`cd` into the `master` directory and run `go run main.go`

In a new terminal window, `cd` into the worker directory run the following commands
```bash
chmod +x generate.py run.sh 
./generate.py > a.txt 
./generate.py > b.txt 
./generate.py > c.txt

chmod +x run.sh 
./run.sh
```

You will see the output inside `final.txt`

