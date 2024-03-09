namespace DbFun.Core

open System.Data
open System

/// <summary>
/// Provides access to current database connection and transaction.
/// </summary>
type IConnector = 
    inherit IDisposable

    /// <summary>
    /// The database connection.
    /// </summary>
    abstract member Connection: IDbConnection

    /// <summary>
    /// The current transaction (null if there is no active transaction).
    /// </summary>
    abstract member Transaction: IDbTransaction

    /// <summary>
    /// Creates new connector instance with the specified transaction.
    /// </summary>
    abstract member With: IDbTransaction -> IConnector

    abstract member Clone: unit -> IConnector

/// <summary>
/// The minimal IConnector implementation.
/// </summary>
type Connector(createConnection: unit -> IDbConnection, connection: IDbConnection, transaction: IDbTransaction) = 

    new (createConnection: unit -> IDbConnection) = 
        let connection = createConnection()
        connection.Open()
        new Connector(createConnection, connection, null)

    // TODO: refactor it out
    //new (connection: IDbConnection) = 
    //    new Connector((fun () -> failwith "Cloning is not supported"), connection, null)

    // TODO: refactor it out
    //new (connection: IDbConnection, transaction: IDbTransaction) = 
    //    new Connector((fun () -> failwith "Cloning is not supported"), connection, transaction)

    interface IConnector with
        member __.Connection: IDbConnection = connection
        member __.Transaction: IDbTransaction = transaction
        member __.With(transaction: IDbTransaction) = new Connector(createConnection, connection, transaction)
        member __.Clone() = new Connector(createConnection)

    interface IDisposable with
        member this.Dispose(): unit = 
            connection.Dispose()