namespace DbFun.Core

open System.Data

type IConnector = 
    abstract member Connection: IDbConnection
    abstract member Transaction: IDbTransaction
    abstract member With: IDbTransaction -> IConnector

type Connector(connection: IDbConnection, transaction: IDbTransaction) = 

    new (connection: IDbConnection) = Connector(connection, null)

    interface IConnector with
        member __.Connection: IDbConnection = connection
        member __.Transaction: IDbTransaction = transaction
        member __.With(transaction: IDbTransaction) = 
            Connector(connection, transaction)
