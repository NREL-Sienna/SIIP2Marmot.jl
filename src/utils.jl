get_installed_capacity(d::PSY.ThermalGen) = PSY.get_rating(d)
get_installed_capacity(d::PSY.HydroGen) = PSY.get_rating(d)
get_installed_capacity(d::PSY.RenewableGen) = PSY.get_rating(d)
get_installed_capacity(d::PSY.StaticLoad) = PSY.get_max_active_power(d)
get_installed_capacity(d::PSY.HybridSystem) = PSY.get_interconnection_rating(d)
get_installed_capacity(d::PSY.Storage) = PSY.get_rating(d)


function preset_system_unit_settings!(system::PSY.System)
    if PSY.get_units_base(system) != "NATURAL_UNITS"
        PSY.set_units_base_system!(system, PSY.UnitSystem.NATURAL_UNITS)
    end
    return
end

function write_marmot_file(df::DataFrames.DataFrame, save_dir; kwarg...)
    file_format = get(kwargs, :file_format, "csv")
    if file_format == "csv"
        write_marmot_csv_file(df, save_dir)
    elseif file_format == "arrow"
        write_marmot_arrow_file(df, save_dir)
    end
    return
end

function write_marmot_csv_file(df::DataFrames.DataFrame, save_dir)
    CSV.write(save_dir, df)
    return
end

function write_marmot_arrow_file(df::DataFrames.DataFrame, save_dir)
    Arrow.write(save_dir, df)
    return
end

IS.@scoped_enum(
    PowerFlowExport,
    VARIABLE_VALUE_BASED = 1,
    INJECTION_CALCULATION_BASED = 2
)
