# jsonstream
Streaming replacement for JSON.parse and JSON.stringify for NodeJS projects.

## JSONStreamReader

Initialize with optional parameters to set maximum token sizes.
Call start() with a stream in Buffer mode to start parsing.
The following standard events are emitted.
Note: If the 'error' event is emitted, no 'end' event is emitted.

### 'error'

Passes an object of type Error with detailed information about parsing error.

### 'end'

Passes an object of type any that contains the fully parsed object.


### 'close'

Passed after 'end' event when stream is closed.

### reader.emitIncremental(name: string)

If emitIncremental is called, individual properties associated with the given named property are returned
incrementally rather than appended to the aggregate object.

The following events are emitted when encountering a value in such a field.

### 'array'

Passes the name of the field passed to emitIncremental that caused this event and the value that would have
been pushed into the array.

### 'object'

Passes the name of the field passed to emitIncremental that caused this event, the property name that would
have been added to the object and the property value.


## JSONStreamWriter

Initialize with optional parameters and then call start() with a writeable stream and the object to write.
