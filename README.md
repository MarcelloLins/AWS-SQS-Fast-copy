# SQS Mover

Tool to move messages from one SQS Queue to another, using multithreading to make the process faster.

## Quick Explanation of the Process

This tool will fire X threads, where each thread will continually dequeue messages from a source SQS Queue and enqueue it to a target SQS Queue. All queue operations are executed in "Batch" ("EnqueueBatch", "DequeueBatch" and "DeleteBatch"), making sure that not only the speed will increase but also the cost will decrease (since AWS charges per Queue operation).

Once all messages from the source SQS Queue are consumed, all the threads halt and the process ends. Also, to avoid reaching the limit of "messages in flight" (120.000), each thread deletes the messages it dequeued every 1000.

Have in mind that, depending on the network speed of your host, you will have to tune the "Default Visibility Timeout" for your queue, to make sure that the message won't go back to it's source queue before it is deleted by the process. Usually speaking, 1000 messages can get dequeued, requeued and deleted in less than 1 minute, but still, it will depend on your network speed.

## Configuring

Configuration is done via both app.config and CLI arguments.

Some arguments are mandatory, while others are not but have default values set to them, internally.

## List of Parameters

### AWS SQS Parameters

* AWSAccessKey - Mandatory - Don't have default value

* AWSSecretKey - Mandatory - Don't have default value

* SourceQueue - Mandatory - Don't have default value - Name of the "Source" queue

* TargetQueue - Mandatory - Don't have default value - Name of the "Target" queue

### Timed Queue Configuration

* LogThreshold - Optional - Defaults to 10000 - Log will show progress after each X records processed. E.G: If set to 10000, after 10000 records queued into the target queue, it will log the progress

### Multithreading Configuration

* Threads - Optional - Defaults to CPU_CORES * 2 - Number of threads that will be used to dequeue messages from the source SQS