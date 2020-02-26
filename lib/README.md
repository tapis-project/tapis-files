#Transfers

### We should block concurrent transfers on the same base path? 

A `TransferTask` is initiated on a file operation that needs to be completed asynchronously such as a COPY, 
bulk transfer or potentially a move operation. 

ExchangeName is `tapis.files`
Bindings are on `tapis.files.transfers` and `tapis.files.transfers.child`

1) TransfersService initiates the operation by publishing a message on the `files.transfers` queue. 
The message body is a serialized dump of a `TransferTask`


2) The listener receives the message and does an `ls` on the `sourcePath`. 

3) ForEach item in the file listing
     Create a new `TransferTaskChild` and publish that message
     
4) The transfer task child workers receive that message on the `files.transfers.child` queue. 

5) 
    - check if parentTask is cancelled, if so do nothing
    - If child == dir :
        forEach item in listing(child): 
            recursively call self (with new message and path of item)   
    - If child == file : 
        - update the TransferTask `total_bytes` by += file.getSize() 
        - create 2 RemoteDataClients, 1 for source 1 for dest
        - inStream = client1.getStream(path)
        - client2.insert(path, inStream)

RabbitMQ setup is based loosely on 
https://github.com/0x6e6562/hopper/blob/master/Work%20Distribution%20in%20RabbitMQ.markdown

The application starts up an ExecutorPool with N threads for the TransferTask listeners and another
ExecutorService with 10*N threads for the TransferTaskChild listeners. 
    - Need to find good balance of that ratio based on real workloads.  
