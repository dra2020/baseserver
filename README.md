# base
The Serverbase library pulls together the core set of libraries required by the various nodejs-hosted components of DRA2020.
This includes:

- the nodejs hosted server
- serverless lambda functions
- command line utilities run under nodejs

These libraries are all packaged in this repository. Normally they are included like this:

    import { DB, Lambda } from '@dra2020/baseserver';

Then the various functions of the different sets are available under their appropriate symbol.

These libraries are used only on the server (or nodejs-hosted command line utilites):

- [DBAbstract](./docs/dbabstract.md): A database abstraction interface, exposed as FSM classes.
- [Storage](./docs/storage.md): A blob storage abstraction interface, exposed as FSM classes.
- [JS](./docs/jsonstream.md): A set of classes for doing stream-based reading and writing of JSON.
- [FsmFile](./docs/fsmfile.md): A set of utility classes for exposing file operations (e.g. mkdir) as FSM-compatible classes.
- [LogServer](./docs/logserver.md): The server implementation of the logging interface, supports saving the logs up to
a blob storage.
- [S3](./docs/storages3.md): An implementation of the storage abstraction on top of AWS S3, exposed as FSM classes.
- [DBDynamo](./docs/dbdynamo.md): An implementation of the DB abstraction on top of AWS DynamoDB, exposed as FSM classes.
- [Lambda](.docs/lambda.md): An interface for invoking AWS lambda functions in a consistent way, exposed as FSM classes.
