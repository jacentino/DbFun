namespace MoreSqlFun.Core.Tests

open System
open Xunit
open MoreSqlFun.Core.Builders
open MoreSqlFun.TestTools.Models
open MoreSqlFun.TestTools.Mocks
open MoreSqlFun.Core.Builders.GenericGetters
open System.Data

module ResultTests = 

    let provider = BaseGetterProvider<IDataRecord, IDataRecord>(RowsImpl.getDefaultBuilders())
    
    [<Fact>]
    let ``One record``() = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]
                            
                        ]

        let builderParams = provider :> IRowGetterProvider, reader 
        let result = Results.One<User>("") (builderParams)
        let value = result.Read(reader)

        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
        Assert.Equal(expected, value)


    [<Fact>]
    let ``Many records``() = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                                [ 2; "mike"; "mike@gmail.com"; DateTime(2020, 1, 1) ]
                            ]
                            
                        ]

        let builderParams = provider :> IRowGetterProvider, reader 

        let result = Results.Many<User>("") builderParams
        let value = result.Read(reader) |> Seq.toList

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


    [<Fact>]
    let ``Optional record - Some``() = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]                            
                        ]

        let builderParams = provider :> IRowGetterProvider, reader 
        let result = Results.TryOne<User>("") builderParams
        let value = result.Read(reader)

        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
        Assert.Equal(Some expected, value)


    [<Fact>]
    let ``Optional record - None``() = 

        let reader = createDataReaderMock
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [ ]                            
                        ]

        let builderParams = provider :> IRowGetterProvider, reader 
        let result = Results.TryOne<User>("") builderParams
        let value = result.Read(reader)

        Assert.Equal(None, value)


    [<Fact>]
    let ``Multiple results``() = 
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

        let builderParams = provider :> IRowGetterProvider, prototype 
        let result = 
            Results.Multiple(
                Results.One<User>(""),
                Results.Many (Rows.Tuple<int, string>("roleId", "name")))        
                builderParams
        
        let user, roles = result.Read(regular) 
        
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
                
        Assert.Equal(expected, (user, roles |> Seq.toList))


    [<Fact>]
    let ``Mapping``() = 
        
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

        let builderParams = provider :> IRowGetterProvider, prototype 
        let result = 
            (Results.Multiple(
                Results.One<User>(""), 
                Results.Many (Rows.Tuple<int, string>("roleId", "name"))
            ) |> Results.Map (fun (u, r) -> { userId = u.userId; name = u.name; email = u.email; created = u.created; roles = r |> Seq.map snd |> List.ofSeq })
            ) builderParams

        let value = result.Read(regular)

        let expected = 
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                    roles = [ "Administrator"; "Data Analyst"; "Code Reviewer" ]

                }
                
        Assert.Equal(expected, value)