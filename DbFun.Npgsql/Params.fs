namespace DbFun.Npgsql.Builders

open System
open System.Data
open DbFun.Core
open DbFun.Core.Builders
open Npgsql
open NpgsqlTypes

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
                    member __.SetValue(value: 'Arg, command: IDbCommand): unit = 
                        setValue(command, name, elemNpgType, value)
                    member __.SetNull(command: IDbCommand): unit = 
                        setValue(command, name, elemNpgType, DBNull.Value)
                    member __.SetArtificial(command: IDbCommand): unit = 
                        setValue(command, name, elemNpgType, artificialValue)
                }
