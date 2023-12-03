namespace DbFun.Core

open System.Data

type DbCall<'Result> = IConnector -> Async<'Result>

type DbCall() = 

    static let wrapInTransaction (isolationLevel: IsolationLevel option) (f: IConnector -> Async<'T>) (con: IConnector) =
        async {
            if con.Transaction = null then
                use transaction = 
                    isolationLevel 
                    |> Option.map con.Connection.BeginTransaction 
                    |> Option.defaultWith con.Connection.BeginTransaction
                let! value = f(con.With transaction)
                transaction.Commit()
                return value
            else
                return! f(con)
        }

    static member Map (f: 'T -> 'U) (dbCall: DbCall<'T>): DbCall<'U> = 
        fun con ->
            async {
                let! value = dbCall(con)
                return f(value)
            }

    static member Catch (dbCall: DbCall<'T>): DbCall<Choice<'T, exn>> = 
        fun (con: IConnector) ->
            async { 
                return! dbCall(con)
            } |> Async.Catch

    static member FromAsync<'T> (a: Async<'T>) (_: IConnector) = a
        
    static member InTransaction (f: DbCall<'T>): DbCall<'T> = 
        wrapInTransaction None f

    static member InTransactionWith (isolationLevel: IsolationLevel): DbCall<'T> -> DbCall<'T> = 
        wrapInTransaction (Some isolationLevel)

    static member Run (createConnection: unit-> IDbConnection, dbCall: DbCall<'Result>): Async<'Result> = 
        async {
            use connection = createConnection()
            connection.Open()
            let connector = Connector(connection)
            return! dbCall(connector)
        }


module List =
    
    let toDbCall (items: DbCall<'t> list) (con: IConnector): 't list Async = async {
        let mutable result = []
        for dbCall in items do
            let! item = dbCall con
            result <- item :: result
        return result |> List.rev   
    }

module Array =
    
    let toDbCall (items: DbCall<'t> array) (con: IConnector): 't array Async = async {
        let result = Array.zeroCreate items.Length
        for i in 0..items.Length - 1 do
            let! item = (items[i]) con
            result.[i] <- item
        return result 
    }

module Option =

    let toDbCall (value: DbCall<'t> option) (con: IConnector): 't option Async = async {
        match value with
        | Some dbCall ->
            let! value = dbCall con
            return Some value
        | None ->
            return None
    }


