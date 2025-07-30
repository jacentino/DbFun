namespace DbFun.Core.Tests

open System
open System.Data
open FSharp.Control
open Xunit
open DbFun.Core.Builders
open DbFun.FastExpressionCompiler.Compilers
open DbFun.TestTools.Models
open DbFun.TestTools.Mocks
open DbFun.Core.Builders.GenericGetters
open DbFun.Core.Builders.Compilers

module ResultTests = 

    let compilers: ICompiler list = [ LinqExpressionCompiler(); Compiler() ]

    let providers = 
        compilers 
        |> List.map (fun compiler -> [| BaseGetterProvider<IDataRecord, IDataRecord>(RowsImpl.getDefaultBuilders(), Compiler()) |])
        
    
    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``One record``(provider: IRowGetterProvider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]
                            
                        ]

        let builderParams = provider, reader 
        let result = Results.Single<User>("") (builderParams)
        let value = result.Read(reader) |> Async.RunSynchronously

        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
        Assert.Equal(expected, value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Many records``(provider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                                [ 2; "mike"; "mike@gmail.com"; DateTime(2020, 1, 1) ]
                            ]
                            
                        ]

        let builderParams = provider, reader 

        let result = Results.List<User>("") builderParams
        let value = result.Read(reader) |> Async.RunSynchronously 

        let expected = 
            [
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
                {
                    userId = 2
                    name = "mike"
                    email = "mike@gmail.com"
                    created = DateTime(2020, 1, 1)
                }
            ]
        Assert.Equal<User list>(expected, value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Many records AsyncSeq``(provider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                                [ 2; "mike"; "mike@gmail.com"; DateTime(2020, 1, 1) ]
                            ]
                            
                        ]

        let builderParams = provider, reader 

        let result = Results.AsyncSeq<User>("") builderParams
        let value = result.Read(reader) |> Async.RunSynchronously 

        let expected = 
            [
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
                {
                    userId = 2
                    name = "mike"
                    email = "mike@gmail.com"
                    created = DateTime(2020, 1, 1)
                }
            ]

        let valueList = value |> AsyncSeq.toListAsync |> Async.RunSynchronously

        Assert.Equal<User list>(expected, valueList)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Optional record - Some``(provider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]                            
                        ]

        let builderParams = provider :> IRowGetterProvider, reader 
        let result = Results.Optional<User>("") builderParams
        let value = result.Read(reader) |> Async.RunSynchronously

        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
        Assert.Equal(Some expected, value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Optional record - None``(provider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [ ]                            
                        ]

        let builderParams = provider, reader 
        let result = Results.Optional<User>("") builderParams
        let value = result.Read(reader) |> Async.RunSynchronously

        Assert.Equal(None, value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Multiple results``(provider) = 
        let prototype, regular = 
            createPrototypeAndRegular
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                    [ col<int> "roleId"; col<string> "name"; ],
                    [
                        [ 1; "Administrator" ]
                        [ 2; "Data Analyst" ]
                        [ 3; "Code Reviewer" ]
                    ]
                ]

        let builderParams = provider, prototype 
        let result = 
            Results.Multiple(
                Results.Single<User>(""),
                Results.List (Rows.Tuple<int, string>("roleId", "name")))        
                builderParams
        
        let user, roles = result.Read(regular) |> Async.RunSynchronously
        
        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                },
                [
                    1, "Administrator"
                    2, "Data Analyst" 
                    3, "Code Reviewer"
                ]
                
        Assert.Equal(expected, (user, roles))


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Mapping``(provider) = 
        
        let prototype, regular = 
            createPrototypeAndRegular
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                    [ col<int> "roleId"; col<string> "name"; ],
                    [
                        [ 1; "Administrator" ]
                        [ 2; "Data Analyst" ]
                        [ 3; "Code Reviewer" ]
                    ]
                ]

        let builderParams = provider, prototype 
        let result = 
            (Results.Multiple(
                Results.Single<User>(""), 
                Results.Seq (Rows.Tuple<int, string>("roleId", "name"))
            ) |> Results.Map (fun (u, r) -> { userId = u.userId; name = u.name; email = u.email; created = u.created; roles = r |> Seq.map snd |> List.ofSeq })
            ) builderParams

        let value = result.Read(regular) |> Async.RunSynchronously

        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                    roles = [ "Administrator"; "Data Analyst"; "Code Reviewer" ]

                }
                
        Assert.Equal(expected, value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Auto - one record``(provider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]
                            
                        ]

        let builderParams = provider, reader 
        let result = Results.Auto<User>() (builderParams)
        let value = result.Read(reader) |> Async.RunSynchronously

        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
        Assert.Equal(expected, value)



    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Auto - optional record``(provider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]                            
                        ]

        let builderParams = provider :> IRowGetterProvider, reader 
        let result = Results.Auto<User option>() builderParams
        let value = result.Read(reader) |> Async.RunSynchronously

        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
        Assert.Equal(Some expected, value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Auto - many records``(provider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                                [ 2; "mike"; "mike@gmail.com"; DateTime(2020, 1, 1) ]
                            ]
                            
                        ]

        let builderParams = provider, reader 

        let result = Results.Auto<User list>() builderParams
        let value = result.Read(reader) |> Async.RunSynchronously 

        let expected = 
            [
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
                {
                    userId = 2
                    name = "mike"
                    email = "mike@gmail.com"
                    created = DateTime(2020, 1, 1)
                }
            ]
        Assert.Equal<User list>(expected, value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Auto - many records AsyncSeq``(provider) = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                                [ 2; "mike"; "mike@gmail.com"; DateTime(2020, 1, 1) ]
                            ]
                            
                        ]

        let builderParams = provider, reader 

        let result = Results.Auto<User AsyncSeq>() builderParams
        let value = result.Read(reader) |> Async.RunSynchronously 

        let expected = 
            [
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
                {
                    userId = 2
                    name = "mike"
                    email = "mike@gmail.com"
                    created = DateTime(2020, 1, 1)
                }
            ]

        let valueList = value |> AsyncSeq.toListAsync |> Async.RunSynchronously

        Assert.Equal<User list>(expected, valueList)
