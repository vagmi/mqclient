Backpressure based queue consumption

## TODO
* [x] publish messages to a named queue with various rates (default n per min)
* [x] Retrieve (n) messages from the queue sleep for 1 minute and continue


## USAGE

For publishing


    mqclient --publish --rate 300


For subscribing


    mqclient --subscribe --rate 200


This will show you that the subscription is rate limited and there is a backlog in the queue. This way you can apply backpressure during queue consumption.
