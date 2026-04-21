let

    // ============================================
    // Snowflake connector
    // ============================================
    
    // Connect to Snowflake
    Source = Snowflake.Databases(
        "***********.snowflakecomputing.com", // Snowflake URL
        "DWH",
        [Implementation = "2.0"]
    ),

    // Select the database
    Db = Source{[Name = "DATABASE", Kind = "Database"]}[Data], // Snowflake DB

    // SQL query
    Query = "
        SELECT *
        FROM 
        WHERE 
    ",

    // Execute native SQL
    NativeFiltered = Value.NativeQuery(Db, Query),

    // ============================================
    // Clean overflowing dates if needed
    // ============================================

    // Cutt offs
    Today = Date.From(DateTime.LocalNow()),
    FutureCutoff = Date.AddYears(Today, 5),
    AncientCutoff = #date(1900, 1, 1),



    // NORMALISATION FUNCTION 
    FixDateTime = (v as any) as nullable datetime =>
        let
            dt = try DateTime.FromText(Text.From(v)) otherwise null,
            d  = if dt <> null then Date.From(dt) else null,

            cleaned =
                if d = null then
                    null
                else if d > FutureCutoff then
                    null
                else if d < AncientCutoff then
                    null
                else
                    dt
        in
            cleaned,

    // APPLY CLEANING TO COL
    DateClean =
        Table.TransformColumns(
            NativeFiltered,
            {
                {
                    "", \\ Date col
                    each FixDateTime(_),
                    type datetime
                }
            }
        ),
        
    // ============================================
    // ADD LOAD DATE
    // ============================================
    
    LoadDate =
        Table.AddColumn(
            DateClean, 
            "LoadDate", 
            each DateTime.LocalNow()
        ),

    // ============================================
    // FINAL TYPE CLEANUP
    // ============================================
    
    SetDTypes =
        Table.TransformColumnTypes(
            LoadDate, {
                {"LoadDate",    type datetime}, 
                {"LockVersion", Int64.Type}
            }
        )

in
    SetDTypes
