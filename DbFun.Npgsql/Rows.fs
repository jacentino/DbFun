namespace DbFun.Npgsql.Builders

open System
open System.Data
open System.Linq.Expressions
open DbFun.Core
open DbFun.Core.Builders
open Microsoft.FSharp.Reflection


module RowsImpl = 

    let getItemType(ordinal: int, schema: DataTable) = 
        match schema.Rows[ordinal].Field<string>(24) with
        | "cmallint[]"                      -> typeof<int16>
        | "integer[]"                       -> typeof<int32>
        | "bigint[]"                        -> typeof<int64>
        | "text[]"                          -> typeof<string>
        | "boolean[]"                       -> typeof<bool>
        | "real[]"                          -> typeof<float>
        | "double precision[]"              -> typeof<double>
        | "numeric[]"                       -> typeof<decimal>
        | "char[]"                          -> typeof<byte>
        | "time with time zone[]"        
        | "timestamp with time zone[]"        
        | "time without time zone[]"        
        | "timestamp without time zone[]"   -> typeof<DateTime>
        | "interval[]"                      -> typeof<TimeSpan>
        | "uuid[]"                          -> typeof<Guid>
        | _                                 -> typeof<obj>
        
    type ArrayColumnBuilder() = 

        member __.GetArray<'SourceItem, 'TargetItem>(ordinal: int, convert: Func<'SourceItem, 'TargetItem>) = 
            fun (record: IDataRecord) ->
                let source = record.GetValue(ordinal) :?> array<'SourceItem>
                source |> Array.map convert.Invoke

        member __.GetChar(value: string) = value.[0]

        interface RowsImpl.IBuilder with
                
            member __.CanBuild(argType: Type): bool = 
                argType.IsArray && Types.isSimpleType(argType.GetElementType())
                    
            member this.Build(name: string, _, prototype: IDataRecord): IRowGetter<'Result> = 
                let ordinal = 
                    try
                        if String.IsNullOrEmpty(name) && prototype.FieldCount = 1 then
                            0
                        else
                            prototype.GetOrdinal(name)
                    with ex -> raise <| Exception(sprintf "Column doesn't exist: %s" name, ex)
                let srcItemType = getItemType(ordinal, (prototype :?> IDataReader).GetSchemaTable())
                let destItemType = Types.getElementType typeof<'Result>
                let param = Expression.Parameter(srcItemType)
                let conversion = 
                    if destItemType = typeof<char> && srcItemType = typeof<string> then
                        let getCharMethod = this.GetType().GetMethod("GetChar")
                        Expression.Call(Expression.Constant(this), getCharMethod, param) :> Expression
                    elif destItemType <> srcItemType then
                        try
                            Expression.Convert(param, destItemType) :> Expression
                        with :? InvalidOperationException as ex ->
                            raise <| Exception(sprintf "Column type doesn't match field type: %s (%s -> %s)" name srcItemType.Name destItemType.Name, ex)
                    else
                        param :> Expression
                let convert = Expression.Lambda(conversion, param).Compile()
                let getterMethod = this.GetType().GetMethod("GetArray").MakeGenericMethod(srcItemType, destItemType)
                let getter = getterMethod.Invoke(this, [| ordinal; convert |]) :?> IDataRecord -> 'Result
                { new IRowGetter<'Result> with
                    member __.Get(record: IDataRecord): 'Result = 
                        getter(record)
                    member __.IsNull(record: IDataRecord): bool = 
                        record.IsDBNull(ordinal)
                    member this.Create(_: IDataRecord): unit = 
                        raise (System.NotImplementedException())
                }

    type ArrayCollectionConverter() = 

        member __.CreateListGetter<'Item>(name: string, provider: IRowGetterProvider, prototype: IDataRecord) = 
            let arrayGetter = provider.Getter<'Item array>(name, prototype)
            { new IRowGetter<'Item list> with
                  member __.Get(record: IDataRecord): 'Item list = 
                      arrayGetter.Get(record) |> Array.toList
                  member __.IsNull(record: IDataRecord): bool = 
                      arrayGetter.IsNull(record)
                  member __.Create(record: IDataRecord): unit = 
                      arrayGetter.Create(record)
            }

        member __.CreateSeqGetter<'Item>(name: string, provider: IRowGetterProvider, prototype: IDataRecord) = 
            let arrayGetter = provider.Getter<'Item array>(name, prototype)
            { new IRowGetter<'Item seq> with
                  member __.Get(record: IDataRecord): 'Item seq = 
                      arrayGetter.Get(record) 
                  member __.IsNull(record: IDataRecord): bool = 
                      arrayGetter.IsNull(record)
                  member __.Create(record: IDataRecord): unit = 
                      arrayGetter.Create(record)
            }

        interface RowsImpl.IBuilder with

            member __.CanBuild(argType: Type): bool = 
                let iscoll = Types.isCollectionType argType 
                let isarray  = argType.IsArray
                iscoll && not isarray

            member this.Build(name: string, provider: IRowGetterProvider, prototype: IDataRecord): IRowGetter<'Result> = 
                let itemType = Types.getElementType typeof<'Result>
                let getterName = if typedefof<'Result> = typedefof<List<_>> then "CreateListGetter" else "CreateSeqGetter"
                let method = this.GetType().GetMethod(getterName).MakeGenericMethod(itemType)
                method.Invoke(this, [| name; provider; prototype |]) :?> IRowGetter<'Result>


    type ArrayItemConverter<'SourceItem, 'DestItem>(convert: 'SourceItem -> 'DestItem) = 

        member __.CreateGetter(name: string, provider: IRowGetterProvider, prototype: IDataRecord) = 
            let arrayGetter = provider.Getter<'SourceItem array>(name, prototype)
            { new IRowGetter<'DestItem array> with
                  member __.Get(record: IDataRecord): 'DestItem array = 
                      arrayGetter.Get(record) |> Array.map convert
                  member __.IsNull(record: IDataRecord): bool = 
                      arrayGetter.IsNull(record)
                  member __.Create(record: IDataRecord): unit = 
                      arrayGetter.Create(record)
            }

        interface RowsImpl.IBuilder with

            member __.CanBuild(argType: Type): bool = 
                argType.IsArray && typeof<'SourceItem>.IsAssignableFrom(argType.GetElementType())

            member this.Build(name: string, provider: IRowGetterProvider, prototype: IDataRecord): IRowGetter<'Result> = 
                let method = this.GetType().GetMethod("CreateGetter")
                method.Invoke(this, [| name; provider; prototype |]) :?> IRowGetter<'Result>


    type EnumArrayConverter<'Underlying>() = 

        member __.CreateGetter<'Enum>(name: string, provider: IRowGetterProvider, prototype: IDataRecord, convert: Func<'Underlying, 'Enum>) = 
            let arrayGetter = provider.Getter<'Underlying array>(name, prototype)
            { new IRowGetter<'Enum array> with
                  member __.Get(record: IDataRecord): 'Enum array = 
                      arrayGetter.Get(record) |> Array.map convert.Invoke
                  member __.IsNull(record: IDataRecord): bool = 
                      arrayGetter.IsNull(record)
                  member __.Create(record: IDataRecord): unit = 
                      arrayGetter.Create(record)
            }

        interface RowsImpl.IBuilder with

            member __.CanBuild(argType: Type): bool = 
                if not argType.IsArray then
                    false
                else
                    let itemType = argType.GetElementType()
                    itemType.IsEnum && itemType.GetEnumUnderlyingType() = typeof<'Underlying>

            member this.Build(name: string, provider: IRowGetterProvider, prototype: IDataRecord): IRowGetter<'Result> = 
                let itemType = typeof<'Result>.GetElementType()
                let underlyingParam = Expression.Parameter(typeof<'Underlying>)
                let convert = Expression.Lambda(Expression.Convert(underlyingParam, itemType), underlyingParam).Compile()                    
                let method = this.GetType().GetMethod("CreateGetter").MakeGenericMethod(itemType)
                method.Invoke(this, [| name; provider; prototype; convert |]) :?> IRowGetter<'Result>


    type UnionArrayConverter<'Underlying>() = 

        member __.CreateGetter<'Union>(name: string, provider: IRowGetterProvider, prototype: IDataRecord) = 
            let mapping = 
                [| for uc in FSharpType.GetUnionCases typeof<'Union> do
                    let ucTag = uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>)[0] :?> Models.UnionCaseTagAttribute
                    let prop = typeof<'Union>.GetProperty(uc.Name)
                    ucTag.Value, prop.GetValue(null) :?> 'Union
                |] |> readOnlyDict
            let arrayGetter = provider.Getter<string array>(name, prototype)
            { new IRowGetter<'Union array> with
                  member __.Get(record: IDataRecord): 'Union array = 
                      arrayGetter.Get(record) |> Array.map (fun tag -> mapping[tag])
                  member __.IsNull(record: IDataRecord): bool = 
                      arrayGetter.IsNull(record)
                  member __.Create(record: IDataRecord): unit = 
                      arrayGetter.Create(record)
            }

        interface RowsImpl.IBuilder with

            member __.CanBuild(argType: Type): bool = 
                if not argType.IsArray then
                    false
                else
                    let itemType = argType.GetElementType()
                    FSharpType.IsUnion itemType
                        && 
                    itemType
                    |> FSharpType.GetUnionCases
                    |> Seq.forall (fun uc -> not (Seq.isEmpty (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>))))

            member this.Build(name: string, provider: IRowGetterProvider, prototype: IDataRecord): IRowGetter<'Result> = 
                let method = this.GetType().GetMethod("CreateGetter").MakeGenericMethod(typeof<'Result>.GetElementType())
                method.Invoke(this, [| name; provider; prototype |]) :?> IRowGetter<'Result>




