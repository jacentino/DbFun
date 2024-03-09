namespace DbFun.Core.Tests

open Xunit
open DbFun.Core
open Moq
open System.Data

module DbCallTests = 

    let run (f: DbCall<'T>) = 
        let connection = Mock<IDbConnection>()
        let connector = new Connector((fun () -> failwith "Cloning is not supported"), connection.Object, null)
        f(connector)

    let runWithMocks (connection: IDbConnection, transaction: IDbTransaction) (f: DbCall<'T>) = 
        let connector = new Connector((fun () -> failwith "Cloning is not supported"), connection, transaction)
        f(connector)

    let toDbCall x = dbsession { return x }


    [<Fact>]
    let ``List toDbCall``() = 
    
        let listOfDbCalls = [ toDbCall 1; toDbCall 3; toDbCall 12; toDbCall 4; toDbCall 7 ]
        
        let result = listOfDbCalls |> List.toDbCall |> run |> Async.RunSynchronously

        Assert.Equal<List<int>>([ 1; 3; 12; 4; 7 ], result)
        

    [<Fact>]
    let ``Array toDbCall``() = 
    
        let arrayOfDbCalls = [| toDbCall 1; toDbCall 3; toDbCall 12; toDbCall 4; toDbCall 7 |]
        
        let result = arrayOfDbCalls |> Array.toDbCall |> run |> Async.RunSynchronously

        Assert.Equal<array<int>>([| 1; 3; 12; 4; 7 |], result)
        

    [<Fact>]
    let ``Option toDbCall Some``() = 
        
        let optionOfDbCall = Some (toDbCall 1)

        let result = optionOfDbCall |> Option.toDbCall |> run |> Async.RunSynchronously

        Assert.Equal(Some 1, result)
        

    [<Fact>]
    let ``Option toDbCall None``() = 
        
        let result = None |> Option.toDbCall |> run |> Async.RunSynchronously

        Assert.Equal(None, result)


    [<Fact>]
    let ``DbCall InTransaction creates and passes transaction``() = 
        
        let connection = Mock<IDbConnection>()
        let transaction = Mock<IDbTransaction>()
        connection.Setup(fun x -> x.BeginTransaction()).Returns(transaction.Object) |> ignore
        let txnExists = ref false

        let f (con: IConnector) = async { txnExists.Value <- con.Transaction <> null }

        dbsession {
            do! f
        } |> DbCall.InTransaction |> runWithMocks(connection.Object, null) |> Async.RunSynchronously
        
        Assert.True(txnExists.Value)
        connection.Verify((fun x -> x.BeginTransaction()), Times.Once)
        transaction.Verify((fun x -> x.Commit()), Times.Once)


    [<Fact>]
    let ``DbCall InTransaction doesn't create transaction if one already exists``() = 
        
        let connection = Mock<IDbConnection>()
        let transaction = Mock<IDbTransaction>()
        connection.Setup(fun x -> x.BeginTransaction()).Returns(transaction.Object) |> ignore
        let txnExists = ref false

        let f (con: IConnector) = async { txnExists.Value <- con.Transaction <> null }

        dbsession {
            do! f
        } |> DbCall.InTransaction |> runWithMocks(connection.Object, transaction.Object) |> Async.RunSynchronously
        
        Assert.True(txnExists.Value)
        connection.Verify((fun x -> x.BeginTransaction()), Times.Never)
        transaction.Verify((fun x -> x.Commit()), Times.Never)


    [<Fact>]
    let ``DbCall InTransactionWith creates and passes transaction``() = 
        
        let connection = Mock<IDbConnection>()
        let transaction = Mock<IDbTransaction>()
        connection.Setup(fun x -> x.BeginTransaction(IsolationLevel.RepeatableRead)).Returns(transaction.Object) |> ignore
        let txnExists = ref false

        let f (con: IConnector) = async { txnExists.Value <- con.Transaction <> null }

        dbsession {
            do! f
        } |> DbCall.InTransactionWith IsolationLevel.RepeatableRead |> runWithMocks(connection.Object, null) |> Async.RunSynchronously
        
        Assert.True(txnExists.Value)
        connection.Verify((fun x -> x.BeginTransaction(IsolationLevel.RepeatableRead)), Times.Once)
        transaction.Verify((fun x -> x.Commit()), Times.Once)


    [<Fact>]
    let ``DbCall InTransactionWith doesn't create transaction if one already exists``() = 
        
        let connection = Mock<IDbConnection>()
        let transaction = Mock<IDbTransaction>()
        connection.Setup(fun x -> x.BeginTransaction(IsolationLevel.RepeatableRead)).Returns(transaction.Object) |> ignore
        let txnExists = ref false

        let f (con: IConnector) = async { txnExists.Value <- con.Transaction <> null }

        dbsession {
            do! f
        } |> DbCall.InTransactionWith IsolationLevel.RepeatableRead |> runWithMocks(connection.Object, transaction.Object) |> Async.RunSynchronously
        
        Assert.True(txnExists.Value)
        connection.Verify((fun x -> x.BeginTransaction()), Times.Never)
        transaction.Verify((fun x -> x.Commit()), Times.Never)


    [<Fact>]
    let ``DbCall InTransaction in case of any exception transaction is not committed``() = 
        
        let connection = Mock<IDbConnection>()
        let transaction = Mock<IDbTransaction>()
        connection.Setup(fun x -> x.BeginTransaction()).Returns(transaction.Object) |> ignore

        let f (con: IConnector) = async { failwith "DbCall processing exception" }

        try
            dbsession {
                do! f
            } |> DbCall.InTransaction |> runWithMocks(connection.Object, null) |> Async.RunSynchronously
        with _ ->
            // Ignored intentionally
        
        transaction.Verify((fun x -> x.Commit()), Times.Never)


    [<Fact>]
    let ``DbCall Catch returns exception``() = 
        
        let f (con: IConnector) = async { failwith "DbCall processing exception" }

        let result = 
            dbsession {
                do! f
            } |> DbCall.Catch |> run |> Async.RunSynchronously

        match result with
        | Choice1Of2 () -> Assert.True(false)
        | Choice2Of2 ex -> Assert.Equal("DbCall processing exception", ex.Message)


    [<Fact>]
    let ``DbCall Catch returns result if no exception occured``() = 
        
        let f (con: IConnector) = async { return 1 }

        let result = 
            dbsession {
                return! f
            } |> DbCall.Catch |> run |> Async.RunSynchronously

        match result with
        | Choice1Of2 value -> Assert.Equal(1, value)
        | Choice2Of2 _ -> Assert.True(false)