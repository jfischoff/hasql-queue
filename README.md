[![Travis CI Status](https://travis-ci.org/jfischoff/hasql-queue.svg?branch=master)](http://travis-ci.org/jfischoff/hasql-queue)

# hasql-queue

This module utilizes PostgreSQL to implement a durable queue for efficently processing payloads.

Typically a producer would enqueue a new payload as part of larger database transaction

```haskell
createAccount userRecord = transaction Serializable Write $ do
  createUser userRecord
  enqueue "queue_channel" emailEncoder [makeVerificationEmail userRecord]
```

In another thread or process the consumer would drain the queue.

```haskell
  -- Wait for a single new record and try to send the email 5 times for giving
  -- up and marking the payload as failed.
  forever $ withDequeue "queue_channel" conn emailDecoder 5 1 $
    mapM_ sendEmail
```
