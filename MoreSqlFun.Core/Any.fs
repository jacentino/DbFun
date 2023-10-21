namespace Sql2Fun.Core.Builders

[<AutoOpen>]
module Any = 
    let any<'T> = Unchecked.defaultof<'T>
