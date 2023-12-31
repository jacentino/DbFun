﻿namespace DbFun.Core.Builders

open System
open System.Data
open DbFun.Core

type IOutParamGetter<'Result> = GenericGetters.IGetter<IDbCommand, 'Result>

type IOutParamGetterProvider = GenericGetters.IGetterProvider<unit, IDbCommand>

type OutParamSpecifier<'Result> = IOutParamGetterProvider * unit -> IOutParamGetter<'Result>

module OutParamsImpl = 

    type IBuilder = GenericGetters.IBuilder<unit, IDbCommand>


    type SimpleOutParamBuilder() = 

        let dbTypes = 
            [
                typeof<bool>,       DbType.Boolean
                typeof<byte[]>,     DbType.Binary
                typeof<byte>,       DbType.Byte
                typeof<DateTime>,   DbType.DateTime
                typeof<Decimal>,    DbType.Decimal
                typeof<double>,     DbType.Double
                typeof<Guid>,       DbType.Guid
                typeof<int16>,      DbType.Int16
                typeof<int32>,      DbType.Int32
                typeof<Int64>,      DbType.Int64
                typeof<string>,     DbType.String
            ]

        interface IBuilder with
                
            member __.CanBuild(argType: Type): bool = Types.isSimpleType argType
                    
            member __.Build(name: string, _, ()): IOutParamGetter<'Result> = 
                { new IOutParamGetter<'Result> with
                    member __.Create (command: IDbCommand) = 
                        let param = command.CreateParameter()
                        param.ParameterName <- name
                        param.DbType <- dbTypes |> List.tryFind (fst >> (=) typeof<'Result>) |> Option.map snd |> Option.defaultValue DbType.Object
                        param.Direction <- ParameterDirection.Output
                        command.Parameters.Add param |> ignore
                    member __.Get(command: IDbCommand): 'Result = 
                        let ordinal = command.Parameters.IndexOf(name)
                        if ordinal = -1 then
                            failwithf "Output parameter doesn't exist: %s" name
                        let param = command.Parameters.[ordinal] :?> IDataParameter
                        Convert.ChangeType(param.Value, typeof<'Result>) :?> 'Result
                    member __.IsNull(command: IDbCommand): bool = 
                        let ordinal = command.Parameters.IndexOf(name)
                        if ordinal = -1 then
                            failwithf "Output parameter doesn't exist: %s" name
                        let param = command.Parameters.[ordinal] :?> IDataParameter
                        param.Value = DBNull.Value
                }


    type BaseGetterProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>

    type InitialDerivedGetterProvider<'Config> = GenericGetters.InitialDerivedGetterProvider<unit, IDbCommand, 'Config>

    type DerivedGetterProvider<'Config> = GenericGetters.DerivedGetterProvider<unit, IDbCommand, 'Config>

    type UnitBuilder = GenericGetters.UnitBuilder<unit, IDbCommand>

    type SequenceBuilder = GenericGetters.SequenceBuilder<unit, IDbCommand>

    type OptionBuilder = GenericGetters.OptionBuilder<unit, IDbCommand>

    type Converter<'Source, 'Target> = GenericGetters.Converter<unit, IDbCommand, 'Source, 'Target>

    type EnumConverter<'Underlying> = GenericGetters.EnumConverter<unit, IDbCommand, 'Underlying>

    type UnionBuilder = GenericGetters.UnionBuilder<unit, IDbCommand>

    type RecordBuilder = GenericGetters.RecordBuilder<unit, IDbCommand>

    type TupleBuilder = GenericGetters.TupleBuilder<unit, IDbCommand>

    type Configurator<'Config> = GenericGetters.Configurator<unit, IDbCommand, 'Config>


    let getDefaultBuilders(): IBuilder list = SimpleOutParamBuilder() :: GenericGetters.getDefaultBuilders()

/// <summary>
/// Provides methods creating various query output parameter builders of stored procedures.
/// </summary>
type OutParams() =
    inherit GenericGetters.GenericGetterBuilder<unit, IDbCommand>()

/// <summary>
/// The field-to-parameter mapping override.
/// </summary>
type OutParamOverride<'Arg> = GenericGetters.Override<unit, IDbCommand, 'Arg>
