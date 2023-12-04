namespace DbFun.Core

open System.Data

/// <summary>
/// Provides access to current database connection and transaction.
/// </summary>
type IConnector = 

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

/// <summary>
/// The minimal IConnector implementation.
/// </summary>
type Connector(connection: IDbConnection, transaction: IDbTransaction) = 

    new (connection: IDbConnection) = Connector(connection, null)

    interface IConnector with
        member __.Connection: IDbConnection = connection
        member __.Transaction: IDbTransaction = transaction
        member __.With(transaction: IDbTransaction) = 
            Connector(connection, transaction)
