namespace DbFun.MsSql.Builders

open DbFun.Core.Builders
open DbFun.MsSql.Builders
open System.Data
open Microsoft.Data.SqlClient.Server

[<AutoOpen>]
module Extensions = 

    type QueryConfig = 
        {
            Common      : DbFun.Core.Builders.QueryConfig
            TvpBuilders : TableValuedParamsImpl.IBuilder list
        }
        with 
            static member Default(createConnection: unit -> IDbConnection) = 
                let common = DbFun.Core.Builders.QueryConfig.Default(createConnection)
                {
                    Common      = { common with ParamBuilders = ParamsImpl.getDefaultBuilders(createConnection >> unbox) }
                    TvpBuilders = TableValuedParamsImpl.getDefaultBuilders()
                }

            member this.AddRowConverter(converter: 'Source -> 'Target) = 
                { this with Common = this.Common.AddRowConverter(converter) }

            member this.AddTvpBuilder(builder: TableValuedParamsImpl.IBuilder) = 
                let tvpBuilders = builder :: this.TvpBuilders
                let tvpProvider = ParamsImpl.BaseSetterProvider(tvpBuilders)
                let tvpCollBuilder = ParamsImpl.TVPCollectionBuilder(this.Common.CreateConnection, tvpProvider) :> ParamsImpl.IBuilder
                let paramBuilders = this.Common.ParamBuilders |> List.map (function :? ParamsImpl.TVPCollectionBuilder -> tvpCollBuilder | b -> b)
                { this with
                    Common      = { this.Common with ParamBuilders = paramBuilders }
                    TvpBuilders = tvpBuilders
                }

            member this.AddParamConverter(converter: 'Source -> 'Target) = 
                let tvpBuilder = ParamsImpl.Converter<'Source, 'Target>(converter) 
                let tvpSeqBuilder = ParamsImpl.SeqItemConverter<'Source, 'Target>(converter) 
                { this with Common = this.Common.AddParamConverter(converter) }
                    .AddTvpBuilder(tvpBuilder)
                    .AddTvpBuilder(tvpSeqBuilder)


type QueryBuilder(config: QueryConfig) =
    inherit DbFun.Core.Builders.QueryBuilder(config.Common)

    member __.Config = config

    new(createConnection: unit -> IDbConnection) = 
        QueryBuilder(QueryConfig.Default(createConnection))

    member this.Timeout(timeout: int) = 
        QueryBuilder({ this.Config with Common = { this.Config.Common with Timeout = Some timeout } })

    member this.LogCompileTimeErrors() = 
        QueryBuilder({ this.Config with Common = { this.Config.Common with LogCompileTimeErrors = true } })

