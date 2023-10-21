namespace Sql2Fun.Core

open System
open System.Data

type IConnector = 
    abstract member Connection: IDbConnection
    abstract member Transaction: IDbTransaction

type Connector(connection: IDbConnection, transaction: IDbTransaction) = 

    member __.BeginTransaction() =
        new Connector(connection, connection.BeginTransaction())

    member __.Commit() = 
        transaction.Commit()

    interface IConnector with
        member __.Connection: IDbConnection = connection
        member __.Transaction: IDbTransaction = transaction
    interface IDisposable with
        member __.Dispose() =
            if transaction <> null then
                transaction.Dispose()
            else
                connection.Dispose()

type DbCmd<'Result> = IConnector -> Async<'Result>
