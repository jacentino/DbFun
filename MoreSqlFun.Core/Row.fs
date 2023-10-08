namespace MoreSqlFun.Core.Builders

open System
open System.Data
open System.Linq.Expressions
open MoreSqlFun.Core

type IRowGetter<'Result> = GenericGetters.IGetter<IDataRecord, 'Result>

type IRowGetterProvider = GenericGetters.IGetterProvider<IDataRecord, IDataRecord>

module RowsImpl = 

    type IBuilder = GenericGetters.IBuilder<IDataRecord, IDataRecord>


    type SimpleColumnBuilder() = 

        static let typedColAccessMethods = 
            [
                typeof<Boolean>,    "GetBoolean"
                typeof<Byte>,       "GetByte"
                typeof<Char>,       "GetChar"
                typeof<DateTime>,   "GetDateTime"
                typeof<Decimal>,    "GetDecimal"
                typeof<Double>,     "GetDouble"
                typeof<float>,      "GetFloat"
                typeof<Guid>,       "GetGuid"
                typeof<Int16>,      "GetInt16"
                typeof<Int32>,      "GetInt32"
                typeof<Int64>,      "GetInt64" 
                typeof<string>,     "GetString"
            ] 
            |> List.map (fun (t, name) -> t, typeof<IDataRecord>.GetMethod(name))                

        static let getValueMethod = typeof<IDataRecord>.GetMethod("GetValue")

        interface IBuilder with
                
            member __.CanBuild(argType: Type): bool = Types.isSimpleType argType
                    
            member __.Build(_, name: string) (prototype: IDataRecord): IRowGetter<'Result> = 
                let ordinal = 
                    try
                        prototype.GetOrdinal(name)
                    with ex -> raise <| Exception(sprintf "Column doesn't exist: %s" name, ex)
                let fieldType = prototype.GetFieldType(ordinal)
                let colGetter = typedColAccessMethods |> List.tryFind (fst >> (=) fieldType) |> Option.map snd |> Option.defaultValue getValueMethod
                let recParam = Expression.Parameter(typeof<IDataRecord>)
                let call = Expression.Call(recParam, colGetter, Expression.Constant(ordinal))
                let convertedCall = 
                    if typeof<'Result> <> call.Type then
                        try
                            Expression.Convert(call, typeof<'Result>) :> Expression
                        with :? InvalidOperationException as ex ->
                            raise <| Exception(sprintf "Column type doesn't match field type: %s (%s -> %s)" name call.Type.Name typeof<'Result>.Name, ex)
                    else
                        call :> Expression
                let getter = Expression.Lambda<Func<IDataRecord, 'Result>>(convertedCall, recParam).Compile()
                { new IRowGetter<'Result> with
                        member __.Get(record: IDataRecord): 'Result = 
                            getter.Invoke(record)
                        member __.IsNull(record: IDataRecord): bool = 
                            record.IsDBNull(ordinal)
                        member this.Create(arg1: IDataRecord): unit = 
                            raise (System.NotImplementedException())
                }

    

    type KeySpecifier<'Primary, 'Foreign>(primary: 'Primary, foreign: 'Foreign) =
            member __.Primary = primary
            member __.Foreign = foreign


    let getDefaultBuilders(): IBuilder list = 
        SimpleColumnBuilder() :: GenericGetters.getDefaultBuilders()

open RowsImpl

type Rows() = 
    inherit GenericGetters.GenericGetterBuilder<IDataRecord, IDataRecord>()

    static member Key<'Result1, 'Result2>(name1: string, name2: string): IRowGetterProvider * IDataRecord -> IRowGetter<KeySpecifier<'Result1, 'Result2>> =
        fun (provider: IRowGetterProvider, prototype: IDataRecord) ->
            let getter1 = provider.Getter<'Result1>(name1, prototype)
            let getter2 = provider.Getter<'Result2>(name2, prototype)
            { new IRowGetter<KeySpecifier<'Result1, 'Result2>> with
                member __.IsNull(record: IDataRecord): bool = 
                    getter1.IsNull(record) && getter2.IsNull(record)
                member __.Get(record: IDataRecord): KeySpecifier<'Result1, 'Result2> = 
                    KeySpecifier(getter1.Get(record), getter2.Get(record))
                member __.Create(record: IDataRecord): unit = 
                    getter1.Create(record)
                    getter2.Create(record)
            }

    static member Key<'Result1, 'Result2>(
            createGetter1: IRowGetterProvider * IDataRecord -> IRowGetter<'Result1>, 
            createGetter2: IRowGetterProvider * IDataRecord -> IRowGetter<'Result2>)
            : IRowGetterProvider * IDataRecord -> IRowGetter<KeySpecifier<'Result1, 'Result2>> = 
        fun (provider: IRowGetterProvider, prototype: IDataRecord) ->
            let getter1 = createGetter1(provider, prototype)
            let getter2 = createGetter2(provider, prototype)
            { new IRowGetter<KeySpecifier<'Result1, 'Result2>> with
                member __.IsNull(record: IDataRecord): bool = 
                    getter1.IsNull(record) && getter2.IsNull(record)
                member __.Get(record: IDataRecord): KeySpecifier<'Result1, 'Result2> = 
                    KeySpecifier(getter1.Get(record), getter2.Get(record))
                member __.Create(record: IDataRecord): unit = 
                    getter1.Create(record)
                    getter2.Create(record)
            }

    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, resultName: string) = 
        Rows.Tuple(Rows.Key<'Primary, 'Foreign>(primaryName, foreignName), Rows.Simple<'Result>(resultName))

    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Rows.Tuple(Rows.Key<'Primary, 'Foreign>(primaryName, foreignName), result)

    static member Keyed<'Primary, 'Foreign, 'Result>(
            primary: IRowGetterProvider * IDataRecord -> IRowGetter<'Primary>, 
            foreign: IRowGetterProvider * IDataRecord -> IRowGetter<'Foreign>, 
            result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Rows.Tuple(Rows.Key(primary, foreign), result)

    static member PK<'Primary, 'Result>(primaryName: string, resultName: string) = 
        Rows.Tuple(Rows.Key<'Primary, unit>(primaryName, ""), Rows.Simple<'Result>(resultName))

    static member PK<'Primary, 'Result>(primaryName: string, result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Rows.Tuple(Rows.Key<'Primary, unit>(primaryName, ""), result)

    static member PK<'Primary, 'Result>(primary: IRowGetterProvider * IDataRecord -> IRowGetter<'Primary>, result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Rows.Tuple(Rows.Key<'Primary, unit>(primary, Rows.Simple<unit>("")), result)

    static member FK<'Foreign, 'Result>(foreignName: string, resultName: string) = 
        Rows.Tuple(Rows.Key<unit, 'Foreign>("", foreignName), Rows.Simple<'Result>(resultName))

    static member FK<'Foreign, 'Result>(foreignName: string, result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Rows.Tuple(Rows.Key<unit, 'Foreign>("", foreignName), result)

    static member FK<'Foreign, 'Result>(foreign: IRowGetterProvider * IDataRecord -> IRowGetter<'Foreign>, result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Rows.Tuple(Rows.Key<unit, 'Foreign>(Rows.Simple<unit>(""), foreign), result)


type RowOverride<'Arg> = GenericGetters.Override<IDataRecord, IDataRecord, 'Arg>