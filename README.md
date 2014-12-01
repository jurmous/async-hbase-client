Async-hbase-client (AHC) is an asynchronous HBase client based mostly on the client code of Hbase itself.
It works with the same Put, Get, Delete, Scan, Increment, Append classes as in the Hbase client. This way it is
 easy to support all current and future features in Hbase.
It works with the same configuration settings as HBase so old configs can be easily recycled. It will
also use the same security features for SASL based authentication.

This project implements the whole RPC layer upon a Netty based stack to be non-blocking. This is
particularly useful in event-driven applications in which blocking threads are unwanted. It also
enables more types of async RPC calls than Hbase enables which is currently mostly row based.

# Download

## Maven
```xml
<dependency>
  <groupId>org.mousio</groupId>
  <artifactId>async-hbase-client</artifactId>
  <version>0.7.1</version>
</dependency>
```

## Gradle
```
compile 'org.mousio:async-hbase-client:0.7.1'
```

## Manually
Visit [AHC Github releases page](https://github.com/jurmous/async-hbase-client/releases)

# Currently supported Hbase version

This implementation is based on Hbase 0.98. But since it is based on the protobuf api it should be
able to communicate with different versions of Hbase using the version 1 api.

# Current status

This project is new and not yet battle tested in all areas. It is based on the Hbase implementation
so it should do all the same things but bugs could still be present. Please report issues and they will be fixed.

Supported:
* Table based calls
  * Get (Single+Multiple)
  * Put (Single+Multiple)
  * Delete (Single+Multiple)
  * Scan
  * Mutate
  * Append
  * Increment
  * increment column value
  * check value before Put/Delete/Mutate commands
  * Coprocessor on single row

Implemented but needs to be checked:
* Security. Simple/Kerberos. It is based on Hbase implementation but refactored to work in a Netty Stack.
* Scans spanning multiple regions. It is based on Hbase implementation but refactored to be Async.
* Call failover. It is based on Hbase implementation but refactored to be Async.

Not yet implemented:
* Coprocessor over multiple rows/regions. Please provide an example since I am currently not using one yet.

**Please report any issues when you encounter them**

# Setup

```Java

Configuration configuration = ...;// Your HBase configuration
connection = HConnectionManager.createConnection(configuration);

client = new HbaseClient(connection);

// Do your magic here

// Don't forget to close the client after use or when you exit your application.
client.close()

```

# A basic Put and Get

```Java
TableName table = ...; // Any reference to a table

Put put = ...; // Any Put command with no limitations

client.put(table, put, new ResponseHandler<Void>() {
  @Override public void onSuccess(Void response) {
    // Successful response. Put does not return anything
  }

  @Override public void onFailure(IOException e) {
    // Failed response. Handle it.
  }
});

Get get = ...; // Any Get command with no limitations

client.put(table, get, new ResponseHandler<Result>() {
  @Override public void onSuccess(Result result) {
    // Successful response. Handle result here
  }

  @Override public void onFailure(IOException e) {
    // Failed response. Handle it.
  }
});

```

# Promise

ResponseHandler class handles all responses. But sometimes you want to call a command in a blocking
way. A promise can be created on the client to handle any responses. The included promise is hooked
to the event loop of the Netty client communicating to HBase. You can also use another type of promise
that fits the rest of your application by extending it and implement ResponseHandler.

```Java

// Both promises fire their Rpc calls immediately on creation, so neither is blocking the other.
HbaseResponsePromise<Void> promise1 = client.put(table1, put1, client.<Void>newPromise());
HbaseResponsePromise<Void> promise2 = client.put(table2, put2, client.<Void>newPromise());

try{
  // Block until result
  promise1.get();
  promise2.get();
}catch(IOException throwable){
  // Handle errors
}
```

# Scanner

Scanners are a bit different in AHC to better fit the async process. It supports all the parameters in
scan and enables you to do small and reversed scans.

**BE AWARE:** Each instance of a scanner which is not set to ```setSmall(true)``` opens a process on the server.
You need to close any instance of the scanner if you don't continue until the end. So close it if you don't
continue after an exception or any other reason with ```scanner.close()```. The scanner is automatically
closed when the scan reached the end row of scan or region or has retrieved its max amount of results
defined by ```scan.setMaxResultSize(int)```.

```Java

Scan scan = new Scan();
// The amount of items to fetch in one batch. By default it will take the HBase default which is 100.
scan.setCaching(50);

final AsyncResultScanner scanner = client.getScanner(TEST_TABLE, scan);

// Fetch the first batch of in this case 50 items
scanner.nextBatch(new ResponseHandler<Result[]>() {
 @Override public void onSuccess(Result[] results) {
    // Do all the work here

    // Continue with the next batch after work is done
    // You can also do it anywhere to ensure the fetching of all batches
    if(!scanner.isScanDone()){
       scanner.nextBatch(this);
    }
 }

 @Override public void onFailure(IOException e) {
   // Failed response. Handle it.
 }
});

```

# More examples

Check out [HbaseClientTest](https://github.com/jurmous/async-hbase-client/blob/master/src/test/java/mousio/hbase/async/HbaseClientTest.java)
for more examples of the API.