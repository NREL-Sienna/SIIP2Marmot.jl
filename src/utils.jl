get_installed_capacity(d::PSY.ThermalGen) = PSY.get_rating(d)
get_installed_capacity(d::PSY.HydroGen) = PSY.get_rating(d)
get_installed_capacity(d::PSY.RenewableGen) = PSY.get_rating(d)
get_installed_capacity(d::PSY.StaticLoad) = PSY.get_max_active_power(d)
get_installed_capacity(d::PSY.HybridSystem) = PSY.get_interconnection_rating(d)
get_installed_capacity(d::PSY.Storage) = PSY.get_rating(d)

has_area(d::PSY.Component) = !isnothing(PSY.get_area(PSY.get_bus(d)))
get_area_name(d::PSY.Bus) = !isnothing(PSY.get_area(d)) ? PSY.get_name(PSY.get_area(d)) : "System"

function preset_system_unit_settings!(system::PSY.System)
    if PSY.get_units_base(system) != "NATURAL_UNITS"
        PSY.set_units_base_system!(system, PSY.UnitSystem.NATURAL_UNITS)
    end
    return
end

function write_marmot_file(df::DataFrames.DataFrame, save_dir; kwargs...)
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

function find_interfaces(sys::PSY.System, branch_filter = PSY.get_available)
    interfaces = Dict{Set,Vector{PSY.ACBranch}}()
    for br in PSY.get_components(PSY.ACBranch, sys, branch_filter)
        from_area = PSY.get_area(PSY.get_from(PSY.get_arc(br)))
        to_area = PSY.get_area(PSY.get_to(PSY.get_arc(br)))
        if from_area != to_area
            key = Set(get_name.([from_area, to_area]))
            if haskey(interfaces, key)
                push!(interfaces[key], br)
            else
                interfaces[key] = [br]
            end
        end
    end
    return interfaces
end

function find_custom_interfaces(sys::PSY.System, bus_to_custom_area, branch_filter = PSY.get_available)
    interfaces = Dict{Set,Vector{PSY.ACBranch}}()
    for br in PSY.get_components(PSY.ACBranch, sys, branch_filter)
        from_area = bus_to_custom_area[PSY.get_number(PSY.get_from(PSY.get_arc(br)))]
        to_area = bus_to_custom_area[PSY.get_number(PSY.get_to(PSY.get_arc(br)))]
        if from_area != to_area
            key = Set([from_area, to_area])
            if haskey(interfaces, key)
                push!(interfaces[key], br)
            else
                interfaces[key] = [br]
            end
        end
    end
    return interfaces
end
