[![Travis CI Status](https://travis-ci.org/jfischoff/hasql-queue.svg?branch=master)](http://travis-ci.org/jfischoff/hasql-queue)

# postgresql-simple-queue

This module utilizes PostgreSQL to implement a durable queue for efficently processing arbitrary payloads which can be represented as JSON.

Typically a producer would enqueue a new payload as part of larger database transaction

```haskell
createAccount userRecord = do
  runDBTSerializable $ do
    createUserDB userRecord
    enqueueDB $ makeVerificationEmail userRecord
```

In another thread or process, the consumer would drain the queue.

```haskell
  forever $ do
    -- Attempt get a payload or block until one is available
    payload <- lock conn

    -- Perform application specifc parsing of the payload value
    case fromJSON $ pValue payload of
      Success x -> sendEmail x -- Perform application specific processing
      Error err -> logErr err

    -- Remove the payload from future processing
    dequeue conn $ pId payload
```

## Installation

```bash
stack install postgresql-simple-queue
```

## Blog
This package was discussed in the blog [Testing PostgreSQL for Fun](https://medium.com/@jonathangfischoff/testing-postgresql-for-fun-af891047e5fc)
