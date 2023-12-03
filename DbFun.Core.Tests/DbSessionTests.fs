namespace DbFun.Core.Tests

open Xunit
open DbFun.Core
open Moq
open System.Data
open System

module DbSessionTests = 

    let run (f: DbCall<'T>) = 
        let connection = Mock<IDbConnection>()
        let connector = Connector(connection.Object)
        f(connector)

    [<Fact>]
    let ``DbSessionBuilder Zero``() =
        
        let testVal = 0

        let f (_: IConnector) = async { return () }

        dbsession { 
            if testVal > 0 then 
                do! f 
        } |> run |> Async.RunSynchronously


    [<Fact>]
    let ``DbSessionBuilder ReturnFrom``() =
        
        let f (_: IConnector) = async { return 1 }

        let result = dbsession { return! f } |> run |> Async.RunSynchronously

        Assert.Equal(1, result)
    

    [<Fact>]
    let ``DbSessionBuilder Combine and Delay``() =
        
        let calls = ref 0
        let f1 (_: IConnector) = async { calls.Value <- calls.Value + 1 }
        let f2 (_: IConnector) = async {calls.Value <- calls.Value + 1 }

        let result = 
            dbsession {
                if calls.Value > 0 then
                    do! f1
                    return 0
                else
                    do! f2
                    return 1
            } |> run |> Async.RunSynchronously 

        Assert.Equal(1, calls.Value)
        Assert.Equal(1, result)

    [<Fact>]
    let ``DbSessionBuilder For``() =
        
        let calls = ref 0
        let f (_: IConnector) = async { calls.Value <- calls.Value + 1 }

        dbsession {
            for i in 1..5 do
                do! f
        } |> run |> Async.RunSynchronously

        Assert.Equal(5, calls.Value)


    [<Fact>]
    let ``DbSessionBuilder Using``() =

        let calls = ref 0
        let fcall = ref 0
        let dispcall = ref 0
        let f (_: IConnector) = async { 
            calls.Value <- calls.Value + 1 
            fcall.Value <- calls.Value
        }
        let makeDisposable() = 
            { new IDisposable with
                member __.Dispose() = 
                    calls.Value <- calls.Value + 1
                    dispcall.Value <- calls.Value
            }

        dbsession {
            use disp = makeDisposable()
            do! f
        } |> run |> Async.RunSynchronously

        Assert.Equal(2, calls.Value)
        Assert.Equal(1, fcall.Value)
        Assert.Equal(2, dispcall.Value)