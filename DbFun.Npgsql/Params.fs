namespace DbFun.Npgsql.Builders

open System
open System.Data
open DbFun.Core
open DbFun.Core.Builders
open Npgsql
open NpgsqlTypes
open System.Linq.Expressions
open Microsoft.FSharp.Reflection

module ParamsImpl =

    type PostgresArrayBuilder() = 
        
        let getNpgSqlDbType t = 
            if t = typeof<int> then NpgsqlDbType.Integer
            elif t = typeof<Int64> then NpgsqlDbType.Bigint
            elif t = typeof<Int16> then NpgsqlDbType.Smallint
            elif t = typeof<bool> then NpgsqlDbType.Boolean
            elif t = typeof<decimal> then NpgsqlDbType.Numeric
            elif t = typeof<DateTime> then NpgsqlDbType.Timestamp
            elif t = typeof<TimeSpan> then NpgsqlDbType.Interval
            elif t = typeof<DateOnly> then NpgsqlDbType.Timestamp
            elif t = typeof<TimeOnly> then NpgsqlDbType.Timestamp
            elif t = typeof<Guid> then NpgsqlDbType.Uuid
            elif t = typeof<string> then NpgsqlDbType.Varchar
            elif t = typeof<double> then NpgsqlDbType.Double
            elif t = typeof<byte[]> then NpgsqlDbType.Bytea
            else failwith <| sprintf "Unmappable type: %O" t

        let setValue(command: IDbCommand, name: string, elemNpgType: NpgsqlDbType, value: obj) = 
            let param = new NpgsqlParameter()
            param.ParameterName <- name
            param.Value <- value
            param.NpgsqlDbType <- NpgsqlDbType.Array ||| elemNpgType
            command.Parameters.Add(param) |> ignore

        member __.GetArtificialValue<'Type>(): obj = 
            if typeof<'Type> = typeof<string> then box [| "" |]
            elif typeof<'Type> = typeof<DateTime> then box [| DateTime.Now |]
            elif typeof<'Type> = typeof<byte[]> then box [||]
            elif typeof<'Type>.IsClass then null
            else box [| Unchecked.defaultof<'Type> |]

        interface ParamsImpl.IBuilder with

            member __.CanBuild(argType: Type): bool = 
                Types.isCollectionType argType && Types.isSimpleType (Types.getElementType argType)

            member this.Build(name: string, _: IParamSetterProvider, _: unit): IParamSetter<'Arg> = 
                let elemType = Types.getElementType typeof<'Arg>
                let elemNpgType = getNpgSqlDbType elemType
                let getArtificialMethod = this.GetType().GetMethod("GetArtificialValue").MakeGenericMethod(elemType)
                let artificialValue = getArtificialMethod.Invoke(this, [||])
                { new IParamSetter<'Arg> with
                    member __.SetValue(value: 'Arg, _: int option, command: IDbCommand): unit = 
                        setValue(command, name, elemNpgType, value)
                    member __.SetNull(_: int option, command: IDbCommand): unit = 
                        setValue(command, name, elemNpgType, DBNull.Value)
                    member __.SetArtificial(_: int option, command: IDbCommand): unit = 
                        setValue(command, name, elemNpgType, artificialValue)
                }


    type SeqItemConverter<'Source, 'Target>(convert: 'Source -> 'Target) =

        member __.MapSeq(source: 'Source seq) = source |> Seq.map convert

        interface ParamsImpl.IBuilder with

            member __.CanBuild (argType: Type) = 
                if not (Types.isCollectionType argType) then
                    false
                else
                    let elemType = Types.getElementType argType                        
                    typeof<'Source>.IsAssignableFrom(elemType)

            member this.Build<'Arg> (name: string, provider: IParamSetterProvider, prototype: unit) = 
                let setter = provider.Setter<'Target seq>(name, prototype)                
                let sourceParam = Expression.Parameter(typeof<'Arg>)
                let seqMapMethod = this.GetType().GetMethod("MapSeq")
                let conversion = Expression.Call(Expression.Constant(this), seqMapMethod, sourceParam)
                let convert' = Expression.Lambda<Func<'Arg, 'Target seq>>(conversion, sourceParam).Compile()
                { new IParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, index: int option, command: IDbCommand) = 
                        setter.SetValue(convert'.Invoke(value), index, command)
                    member __.SetNull(index: int option, command: IDbCommand) = 
                        setter.SetNull(index, command)
                    member __.SetArtificial(index: int option, command: IDbCommand) = 
                        setter.SetArtificial(index, command)
                }


    type EnumSeqConverter<'Underlying>() = 

        member __.MapSeq<'Enum, 'Underlying>(mapper: Func<'Enum, 'Underlying>, sequence: 'Enum seq) = 
            sequence |> Seq.map mapper.Invoke

        interface ParamsImpl.IBuilder with

            member __.CanBuild(argType: Type): bool = 
                if not (Types.isCollectionType argType) then
                    false
                else
                    let elemType = Types.getElementType argType
                    elemType.IsEnum && elemType.GetEnumUnderlyingType() = typeof<'Underlying>

            member this.Build(name: string, provider: IParamSetterProvider, prototype: unit): IParamSetter<'Arg> = 
                let setter = provider.Setter<'Underlying seq>(name, prototype)   
                let enumType = Types.getElementType typeof<'Arg>
                let enumParam = Expression.Parameter(enumType)
                let itemConvert = Expression.Lambda(Expression.Convert(enumParam, typeof<'Underlying>), enumParam)
                let seqMapMethod = this.GetType().GetMethod("MapSeq").MakeGenericMethod(enumType, typeof<'Underlying>)
                let seqParam = Expression.Parameter(typeof<'Arg>)
                let conversion = Expression.Call(Expression.Constant(this), seqMapMethod, itemConvert, seqParam)
                let seqConvert = Expression.Lambda<Func<'Arg, 'Underlying seq>>(conversion, seqParam).Compile()
                { new IParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, index: int option, command: IDbCommand) = 
                        setter.SetValue(seqConvert.Invoke(value), index, command)
                    member __.SetNull(index: int option, command: IDbCommand) = 
                        setter.SetNull(index, command)
                    member __.SetArtificial(index: int option, command: IDbCommand) = 
                        setter.SetArtificial(index, command)
                }
        

    type UnionSeqBuilder() = 

        member __.GetUnderlyingValues<'Enum>() = 
            let properties = typeof<'Enum>.GetProperties()
            let underlyingValues = 
                [ for uc in FSharpType.GetUnionCases typeof<'Enum> do
                    (properties |> Array.find(fun p -> p.Name = uc.Name)).GetValue(null) :?> 'Enum, 
                    (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>)[0] :?> Models.UnionCaseTagAttribute).Value
                ] 
            underlyingValues

        member __.MapSeq<'Enum>(eq: Func<'Enum, 'Enum, bool>, underlyingValues: ('Enum * string) list, sequence: 'Enum seq) = 
            let convert (x: 'Enum): string = underlyingValues |> List.find (fun (k, _) -> eq.Invoke(x, k)) |> snd
            sequence |> Seq.map convert

        interface ParamsImpl.IBuilder with

            member __.CanBuild(argType: Type): bool = 
                if not (Types.isCollectionType argType) then
                    false
                else
                    let elemType = Types.getElementType argType
                    FSharpType.IsUnion elemType
                        && 
                    elemType
                    |> FSharpType.GetUnionCases
                    |> Seq.forall (fun uc -> not (Seq.isEmpty (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>))))

            member this.Build(name: string, provider: IParamSetterProvider, prototype: unit): IParamSetter<'Arg> = 
                let setter = provider.Setter<string seq>(name, prototype)   
                let enumType = Types.getElementType typeof<'Arg>
                let op1 = Expression.Parameter(enumType)
                let op2 = Expression.Parameter(enumType)
                let eq = Expression.Lambda(Expression.Equal(op1, op2), op1, op2)
                let seqParam = Expression.Parameter(typeof<'Arg>)
                let seqMapMethod = this.GetType().GetMethod("MapSeq").MakeGenericMethod(enumType)
                let underlyingValsMethod = this.GetType().GetMethod("GetUnderlyingValues").MakeGenericMethod(enumType)
                let underlyingValues = underlyingValsMethod.Invoke(this, [||])
                let conversion = Expression.Call(Expression.Constant(this), seqMapMethod, eq, Expression.Constant(underlyingValues), seqParam)
                let convert = Expression.Lambda<Func<'Arg, string seq>>(conversion, seqParam).Compile()
                { new IParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, index: int option, command: IDbCommand) = 
                        setter.SetValue(convert.Invoke(value), index, command)
                    member __.SetNull(index: int option, command: IDbCommand) = 
                        setter.SetNull(index, command)
                    member __.SetArtificial(index: int option, command: IDbCommand) = 
                        setter.SetArtificial(index, command)
                }
