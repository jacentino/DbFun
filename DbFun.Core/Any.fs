namespace DbFun.Core.Builders

[<AutoOpen>]
module Any = 

    /// <summary>
    /// Function creating prototype objects used to specify property chains in joins and overrides.
    /// </summary>
    let any<'T> = Unchecked.defaultof<'T>
