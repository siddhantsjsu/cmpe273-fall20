# Lab 2

Implement a master-work pattern to calculate the square root of the numbers.

## Components

```

                       PUSH      PULL      PUSH 
|--------------------| ------> | Worker | -------> |-----------|
| Generator (Master) | ------> | Worker | -------> | Dashboard |
|--------------------| ------> | Worker | -------> |-----------|

```

* Generator

The generator component generates a list of numbers from 0 to 10,000 and sends (PUSH) those numbers to Worker.


* Worker

The worker component listens (PULL) the numbers from the generator in a round robin fashion and calculate a square root of the numbers. Finally, sends the result to the dashboard.


* Dashboard

The dashboard component receives the result from the workers and displays the result to the console.


## Solution

The solution to this problem is based on the Parallel Pipeline pattern explained in the ZeroMQ Guide - 

![Parallel Pipeline](assets/parallelPipeline.png)


### Steps to run
1. run `pipenv install` to install dependencies
2. Open 5 terminal windows
3. Run following command in 1 window - `pipenv run python src/master.py` to run Master
4. Run following command 3 window - `pipenv run python src/worker.py >> output.txt` to run Workers
5. Run following command in 1 window - `pipenv run python src/dashboard.py >> output.txt` to run Dashboard

