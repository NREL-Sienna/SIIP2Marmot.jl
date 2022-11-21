

function export_generation(variables, aux_variables, save_dir; kwargs...)
    active_power_dataframes = filter(x -> startswith(x[1], "ActivePowerVariable") || startswith(x[1], "ActivePowerOutVariable"), variables)
    aux_active_power_dataframes = filter(x -> startswith(x[1], "PowerOutput"), aux_variables)
    active_power_df = Dict()
    for (name, df) in merge(aux_active_power_dataframes, active_power_dataframes)
        for col in names(df)
            if !haskey(active_power_df, col)
                active_power_df[col] = df[:, col]
            else
                @warn("Column name already exist with the name $(col), please make sure dataset has unique generator names")
            end
        end
    end
    df = DataFrame(active_power_df)
    write_marmot_file(
        select!(df, :DateTime, DataFrames.Not(:DateTime)),
        joinpath(save_dir, "generation_actual.csv");
        kwargs
    )
    return
end


function export_commitment(variables, save_dir; kwargs...)
    active_power_dataframes = filter(x -> startswith(x[1], "OnVariable"), variables)
    active_power_df = Dict()
    for (name, df) in active_power_dataframes
        for col in names(df)
            if !haskey(active_power_df, col)
                active_power_df[col] = df[:, col]
            else
                @warn("Column name already exist with the name $(col), please make sure dataset has unique generator names")
            end
        end
    end
    df = DataFrame(active_power_df)
    write_marmot_file(
        select!(df, :DateTime, Not(:DateTime)),
        joinpath(save_dir, "generation_commitment.csv");
        kwargs
    )
    return
end


function find_interfaces(sys, branch_filter = get_available)
    interfaces = Dict{Set,Vector{ACBranch}}()
    for br in get_components(ACBranch, sys, branch_filter)
        from_area = get_area(get_from(get_arc(br)))
        to_area = get_area(get_to(get_arc(br)))
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

function find_custom_interfaces(sys, bus_to_custom_area, branch_filter = get_available)
    interfaces = Dict{Set,Vector{ACBranch}}()
    for br in get_components(ACBranch, sys, branch_filter)
        from_area = bus_to_custom_area[get_number(get_from(get_arc(br)))]
        to_area = bus_to_custom_area[get_number(get_to(get_arc(br)))]
        if from_area != to_area
            key = [from_area, to_area]
            if haskey(interfaces, key)
                push!(interfaces[key], br)
            else
                interfaces[key] = [br]
            end
        end
    end
    return interfaces
end

function export_interface_flow(
    system,
    variables,
    expressions,
    save_dirs,
    branch_filter = get_available;
    kwargs...
)
    line_flows_df = read_power_flow(system, variables, expressions; kwargs...)
    interfaces = find_interfaces(sys, branch_filter)
    interface_flow = Dict()
    interface_flow[:DateTime] = line_flows_df[:, :DateTime]
    for (interface, lines) in interfaces
        interface_flow[interface] = sum(eachcol(line_flows_df[:, lines]))
    end
    interface_flow_df = DataFrame(interface_flow)
    return select!(interface_flow_df, :DateTime, Not(:DateTime))
end

function export_custom_interface_flow(
    system,
    variables,
    expressions,
    bus_to_custom_area,
    save_dirs,
    branch_filter= get_available; 
    kwargs...
)
    line_flows_df = read_power_flow(system, variables, expressions; kwargs...)
    interfaces = find_custom_interfaces(sys, bus_to_custom_area, branch_filter)
    interface_flow = Dict()
    interface_flow[:DateTime] = line_flows_df[:, :DateTime]
    for (interface, lines) in interfaces
        interface_flow[interface] = sum(eachcol(line_flows_df[:, lines]))
    end
    interface_flow_df = DataFrame(interface_flow)
    return select!(interface_flow_df, :DateTime, Not(:DateTime))
end

function read_power_flow(system, variables, expressions; kwargs...)
    active_power_flow_dataframes = filter(x -> startswith(x[1], "FlowActivePower"), variables)
    nodal_injections = filter(x -> startswith(x[1], "ActivePowerBalance__Bus"), expressions)

    if isempty(active_power_flow_dataframes) && isempty(nodal_injections)
        @info("Simultaion Model doesnt contain network models with explicit flow variables, skipping export of reserve contribution")
        return
    elseif !isempty(nodal_injections)
        @info("Simultaion Model doesnt contain network models with explicit flow variables but includes nodal injection expression which will be used to export of reserve contribution")
        return
    end
    active_power_df = Dict()
    for (name, df) in active_power_flow_dataframes
        for col in names(df)
            if !haskey(active_power_df, col)
                active_power_df[col] = df[:, col]
            else
                @warn("Column already exist with the name $(col) in $(name), please make sure dataset has unique generator names")
            end
        end
    end
    df = DataFrame(active_power_df)
    return select!(df, :DateTime, Not(:DateTime))
end

function export_power_flow(system, variables, expressions, save_dirs; kwargs...)
    power_flow_mode = get(kwargs, :power_flow, PowerFlowExport.VARIABLE_VALUE_BASED)
    if power_flow_mode == PowerFlowExport.INJECTION_CALCULATION_BASED
        ptdf = PSY.PTDF(system)
        calculate_power_flow(expressions, ptdf)
        return
    end
    df = read_power_flow(system, variables, expressions; kwargs...)
    write_marmot_file(
        df,
        joinpath(save_dir, "power_flow_actual.csv");
        kwargs
    )
    return
end

function calculate_power_flow(expressions, ptdf; kwargs...)
    length_ts = length(expressions["ActivePowerBalance__Bus"][:, "DateTime"])
    col_names = vcat("DateTime", ptdf.axes[1])
    col_types = vcat([DateTime], repeat([Float64], length(col_names) - 1))
    df = DataFrame([n => Vector{T}(undef, length_ts) for (n, T) in zip(col_names, col_types)], copycols=false)
    df[:, "DateTime"] = expressions["ActivePowerBalance__Bus"][:, "DateTime"]

    net_inj = Matrix(expressions["ActivePowerBalance__Bus"][:, string.(ptdf.axes[2])])
    _flows = net_inj * ptdf.data'

    df[:, string.(ptdf.axes[1])] .= _flows
    write_marmot_file(
        select!(df, :DateTime, Not(:DateTime)),
        joinpath(save_dir, "power_flow_actual.csv");
        kwargs
    )
    return
end

function export_generation_timeseries(parameters, save_dir; kwargs...)
    active_power_dataframes = filter(x -> startswith(x[1], "ActivePowerTimeSeriesParameter") && !occursin("PowerLoad", x[1]), parameters)
    active_power_df = Dict()
    for (name, df) in active_power_dataframes
        for col in names(df)
            if !haskey(active_power_df, col)
                active_power_df[col] = df[:, col]
            else
                @warn("Column name already exist with the name $(col), please make sure dataset has unique generator names")
            end
        end
    end
    df = DataFrame(active_power_df)
    write_marmot_file(
        select!(df, :DateTime, Not(:DateTime)),
        joinpath(save_dir, "generation_availability.csv");
        kwargs
    )
    return
end

function export_reserve_contribution(variables, save_dir; kwargs...)
    active_power_dataframes = filter(x -> startswith(x[1], "ActivePowerReserveVariable"), variables)
    if isempty(active_power_dataframes)
        @info("Simultaion Model doesnt contain service models, skipping export of reserve contribution")
        return
    end
    active_power_df = Dict()
    for (name, df) in active_power_dataframes
        reserve_name = split(name, "__")[end]
        for col in names(df)
            if col == "DateTime" && !haskey(active_power_df, col)
                active_power_df[col] = df[:, col]
            elseif !haskey(active_power_df, col)
                active_power_df["$(col)_$(reserve_name)"] = df[:, col]
            else
                @warn("Column name already exist with the name $(col), please make sure dataset has unique generator names")
            end
        end
    end
    df = DataFrame(active_power_df)
    write_marmot_file(
        select!(df, :DateTime, Not(:DateTime)),
        joinpath(save_dir, "reserve_contribution.csv");
        kwargs
    )
    return
end

function export_net_demand(results, variables, parameters, save_dir; kwargs...)
    load_mapping = Dict()
    load_df = Dict()
    if isempty(PSY.get_components(PSY.Area, results.system))
        pg_data = get_load_data(results.system, aggregation=PSY.System)
    else
        pg_data = get_load_data(results.system, aggregation=PSY.Area)
    end
    for (area, area_df) in pg_data.data
        load_mapping[area] = names(area_df)
    end
    load_df[Symbol("DateTime")] = parameters["ActivePowerTimeSeriesParameter__PowerLoad"][:, "DateTime"]
    for (area, area_map) in load_mapping
        load_df[area] = -1 .* sum(eachcol(parameters["ActivePowerTimeSeriesParameter__PowerLoad"][:, area_map]))
    end
    active_powerin_dataframes = filter(x -> startswith(x[1], "ActivePowerInVariable"), variables)
    for (name, df) in active_powerin_dataframes
        for col in names(df)
            if !haskey(load_df, col)
                load_df[col] = df[:, col]
            else
                @warn("Column name already exist with the name $(col), please make sure dataset has unique generator names")
            end
        end
    end
    write_marmot_file(
        select!(DataFrame(load_df), :DateTime, Not(:DateTime)),
        joinpath(save_dir, "regional_load.csv");
        kwargs
    )
    return
end

function read_installed_capacity(system::PSY.System, save_dir; kwargs...)
    installed_caps = Dict()
    # TODO: expand to all components, including transmission and all types load 
    for gen_type in [PSY.ThermalGen, PSY.RenewableGen, PSY.HydroGen, PSY.Storage, PSY.HybridSystem]
        for g in PSY.get_components(gen_type, system, PSY.get_available)
            installed_caps[PSY.get_name(g)] = get_installed_capacity(g)
        end
    end
    installed_caps["DateTime"] = Dates.year(PSY.get_forecast_initial_timestamp(system))
    return select!(DataFrame(installed_caps), :DateTime, Not(:DateTime))
end

function export_installed_capacity(system::PSY.System, save_dir; kwargs...)
    df = read_installed_capacity(system, save_dir; kwargs)
    write_marmot_file(
        df,
        joinpath(save_dir, "installed_capacity.csv");
        kwargs
    )
    return
end

function export_system_metadata(sys, save_dir; kwargs...)
    save_path = joinpath(save_dir, "metadata.json")
    if isfile(save_path)
        @warn("A metadata.json already exists, which will be overwritten")
        rm(save_path)
    end
    metadata = Dict()
    metadata["Generator_fuel_mapping"] = Dict()
    metadata["Generator_region_mapping"] = Dict()
    metadata["Generator_reserve_mapping"] = Dict()
    for gen_type in [PSY.ThermalGen, PSY.RenewableGen, PSY.HydroGen, PSY.Storage]
        for g in PSY.get_components(gen_type, sys)
            pm = "$(g.prime_mover)"
            gen_type == PSY.ThermalGen ? f = "_" * "$(g.fuel)" : f = ""
            if has_area(g)
                r = PSY.get_name(PSY.get_area(PSY.get_bus(g)))
            else
                r = ""
            end
            metadata["Generator_fuel_mapping"]["$(g.name)"] = "$(pm)$(f)"
            metadata["Generator_region_mapping"]["$(g.name)"] = "$r"
            for s in g.services
                metadata["Generator_reserve_mapping"]["$(g.name)_$(s.name)"] = ["$(g.name)", "$(s.name)",]
            end
        end
    end
    metadata["Regions"] = ["$(a.name)" for a in PSY.get_components(PSY.Area, sys)]
    metadata["Lines"] = Dict()
    for l in PSY.get_components(PSY.Branch, sys)
        if typeof(l) <: PSY.DCBranch
            metadata["Lines"][l.name] = Dict(
                "from" => PSY.get_number(PSY.get_from(PSY.get_arc(l))),
                "to" => PSY.get_number(PSY.get_to(PSY.get_arc(l))),
                "rate_from" => max(abs(PSY.get_active_power_limits_from(l).max), abs(PSY.get_active_power_limits_from(l).min)),
                "rate_to" => max(abs(PSY.get_active_power_limits_to(l).max), abs(PSY.get_active_power_limits_to(l).min)),
                "from_region" => get_area_name(PSY.get_from(PSY.get_arc(l))),
                "to_region" => get_area_name(PSY.get_to(PSY.get_arc(l))),
            )
        else
            metadata["Lines"][l.name] = Dict(
                "from" => PSY.get_number(PSY.get_from(PSY.get_arc(l))),
                "to" => PSY.get_number(PSY.get_to(PSY.get_arc(l))),
                "rate" => PSY.get_rate(l),
                "from_region" => get_area_name(PSY.get_from(PSY.get_arc(l))),
                "to_region" => get_area_name(PSY.get_to(PSY.get_arc(l))),
            )
        end
    end
    open(save_path, "w") do f
        JSON3.pretty(f, metadata)
    end
    return
end


function export_generator_metadata(sys, save_dir; kwargs...)
    save_path = joinpath(dirname(save_dir), "generator_metadata.json")
    if isfile(save_path)
        @warn("A metadata.json already exists, which will be overwritten")
        rm(save_path)
    end
    metadata = Dict()
    metadata["Generator_fuel_mapping"] = Dict()
    for gen_type in [PSY.ThermalGen, PSY.RenewableGen, PSY.HydroGen, PSY.Storage]
        for g in PSY.get_components(gen_type, sys)
            pm = "$(g.prime_mover)"
            gen_type == PSY.ThermalGen ? f = "_" * "$(g.fuel)" : f = ""
            if has_area(g)
                r = PSY.get_name(PSY.get_area(PSY.get_bus(g)))
            else
                r = ""
            end
            metadata["Generator_fuel_mapping"]["$(g.name)"] = "$(pm)$(f)"
        end
    end
    open(save_path, "w") do f
        JSON3.pretty(f, metadata)
    end
    return
end

function export_marmot_inputs(results::PSI.ProblemResults, save_dir, export_partition_results=false; kwargs...)
    system = PSI.get_system(results)
    preset_system_unit_settings!(system)

    variables = PSI.read_realized_variables(results)
    aux_variables = PSI.read_realized_aux_variables(results)
    parameters = PSI.read_realized_parameters(results)
    duals = PSI.read_realized_duals(results)
    expressions = PSI.read_realized_expressions(results)
    if export_partition_results
        export_system_metadata(sys, dirname(save_dir))
    else
        export_system_metadata(sys, save_dir)
    end
    export_generation(variables, aux_variables, save_dir; kwargs)
    export_generation_timeseries(parameters, save_dir; kwargs)
    export_reserve_contribution(variables, save_dir; kwargs)
    export_commitment(variables, save_dir, ; kwargs)

    export_power_flow(system, variables, expressions, save_dir; kwargs)

    export_net_demand(results, parameters, save_dir; kwargs)
    export_installed_capacity(system, save_dir; kwargs)
end


function export_marmot_inputs(results::PSI.SimulationProblemResults, save_dir; kwargs...)
    system = PSI.get_system(results)
    preset_system_unit_settings!(system)

    variables = PSI.read_realized_variables(results)
    aux_variables = PSI.read_realized_aux_variables(results)
    parameters = PSI.read_realized_parameters(results)
    duals = PSI.read_realized_duals(results)
    expressions = PSI.read_realized_expressions(results)

    export_system_metadata(system, save_dir)
    export_generation(variables, aux_variables, save_dir; kwargs)
    export_generation_timeseries(parameters, save_dir; kwargs)
    export_reserve_contribution(variables, save_dir; kwargs)
    export_commitment(variables, save_dir; kwargs)

    export_power_flow(system, variables, expressions, save_dir; kwargs)

    export_net_demand(results, variables, parameters, save_dir; kwargs)
    export_installed_capacity(system, save_dir; kwargs)
end

function export_marmot_inputs(system::PSY.System, save_dir; kwargs...)
    preset_system_unit_settings!(system)
    export_installed_capacity(system, save_dir; kwargs)
    export_system_metadata(system, save_dir)
end
