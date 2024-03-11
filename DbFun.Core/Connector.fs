namespace DbFun.Core

open System.Data
open System

/// <summary>
/// Provides access to current database connection and transaction.
/// </summary>
type IConnector<'Key> = 
    inherit IDisposable

    /// <summary>
    /// The database connection.
    /// </summary>
    abstract member GetConnection: 'Key -> IDbConnection

    /// <summary>
    /// The current transaction (null if there is no active transaction).
    /// </summary>
    abstract member GetTransaction: 'Key -> IDbTransaction

    /// <summary>
    /// Creates new connector instance with the specified transaction.
    /// </summary>
    abstract member With: 'Key * IDbTransaction -> IConnector<'Key>

    abstract member Clone: unit -> IConnector<'Key>

/// <summary>
/// The minimal IConnector implementation.
/// </summary>
type Connector<'Key when 'Key: comparison>(
        createConnection: 'Key -> IDbConnection, 
        connections: ref<list<'Key * IDbConnection>>, 
        transactions: list<'Key * IDbTransaction>) = 

    new (createConnection: 'Key -> IDbConnection) = 
        new Connector<'Key>(createConnection, ref [], [])

    interface IConnector<'Key> with
        member __.GetConnection(key: 'Key) = 
            match connections.Value |> List.tryFind (fst >> (=) key) with
            | Some (_, connection) -> connection
            | None -> 
                let connection = createConnection key
                connections.Value <- (key, connection) :: connections.Value
                connection.Open()
                connection
                
        member __.GetTransaction(key: 'Key) = transactions |> List.tryFind (fst >> (=) key) |> Option.map snd |> Option.defaultValue null
        member __.With(key: 'Key, transaction: IDbTransaction) = new Connector<'Key>(createConnection, connections, (key, transaction) :: transactions)
        member __.Clone() = new Connector<'Key>(createConnection)

    interface IDisposable with
        member __.Dispose(): unit = 
            for _, con in connections.Value do
                con.Dispose()

type IConnector = IConnector<unit>

type Connector = Connector<unit>