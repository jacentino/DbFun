namespace DbFun.Core.Builders

open System
open System.Linq.Expressions

type ICompiler = 
    abstract member Compile<'Function>: body: Expression * [<ParamArray>] args: ParameterExpression[]  -> 'Function when 'Function : null 

type LinqExpressionCompiler() =     
    interface ICompiler with
        member __.Compile(body: Expression, [<ParamArray>] args: ParameterExpression[]): 'Function =                
            Expression.Lambda<'Function>(body, args).Compile()
