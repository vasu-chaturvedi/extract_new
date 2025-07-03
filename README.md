# Dynamic Worker Pool in Go

This project demonstrates a dynamic worker pool in Go. The number of worker goroutines scales up or down at runtime based on the length of the task queue.

## Features
- Manager goroutine monitors the queue and adjusts worker count.
- Each worker processes tasks and exits cleanly when signaled.
- Simple integer-processing tasks for demonstration.
- Logging shows scaling actions and worker activity.

## How to Run

```sh
go run main.go
```

You should see logs showing workers being added/removed and tasks being processed.
