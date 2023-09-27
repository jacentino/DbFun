namespace MoreSqlFun.Core

open System.Data.Common
open System

type IConnector = 
    abstract member Connection: DbConnection
    abstract member Transaction: DbTransaction

type Connector(connection: DbConnection, transaction: DbTransaction) = 

    member __.BeginTransaction() =
        new Connector(connection, connection.BeginTransaction())

    member __.Commit() = 
        transaction.Commit()

    interface IConnector with
        member __.Connection: DbConnection = connection
        member __.Transaction: DbTransaction = transaction
    interface IDisposable with
        member __.Dispose() =
            if transaction <> null then
                transaction.Dispose()
            else
                connection.Dispose()

type DbCmd<'Result> = IConnector -> Async<'Result>
