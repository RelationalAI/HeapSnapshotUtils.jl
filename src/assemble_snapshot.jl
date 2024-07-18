function _read_streamed_nodes(path, nodes)
    io = open(path, "r", lock=false)
    @inbounds try
        for i in 1:length(nodes)
            node_type = read(io, UInt8)
            node_name_index = unsafe_trunc(UInt32, read(io, UInt))
            node_id = read(io, UInt)
            node_self_size = read(io, Int)
            @assert read(io, Int) == 0  # trace_node_id
            @assert read(io, Int8) == 0 # detachedness

            nodes.type[i] = node_type
            nodes.name_index[i] = node_name_index
            nodes.id[i] = node_id
            nodes.self_size[i] = node_self_size
        end
    finally
        close(io)
    end
end

function _read_streamed_edges(path, nodes)
    io = open(path, "r", lock=false)
    resize!(nodes.edges._from_pos, length(nodes.edges))
    @inbounds try
        for i in 1:length(nodes.edges)
            edge_type = read(io, Int8)
            edge_name_index = Base.unsafe_trunc(UInt32, read(io, UInt))
            from_node = read(io, UInt)
            to_node = read(io, UInt)

            nodes.edges.type[i] = edge_type
            nodes.edges.name_index[i] = edge_name_index
            nodes.edges.to_pos[i] = to_node
            nodes.edges._from_pos[i] = from_node
            nodes.edge_count[from_node+true] += true
        end
        cumulative_count = UInt32(0)
        for i in 1:length(nodes.edge_count)
            nodes._back_count[i] = cumulative_count # temporarily use _back_count to store "prev cumulative count"
            cumulative_count += nodes.edge_count[i]
        end
    finally
        close(io)
    end
end

function _get_permuation_vec(nodes)
    perms = Vector{UInt32}(undef, length(nodes.edges))

    i = UInt32(0)
    @inbounds for from_node in nodes.edges._from_pos
        i += true
        idx = (nodes._back_count[from_node+true] += true) # temporarily use _back_count to store "prev cumulative count"
        perms[idx] = i
    end

    fill!(nodes._back_count, 0)
    empty!(nodes.edges._from_pos)
    sizehint!(nodes.edges._from_pos, 0)
    return perms
end


function _get!(default::Base.Callable, h::Dict{K,V}, key) where {K,V}
    index, sh = Base.ht_keyindex2_shorthash!(h, key)

    index > 0 && return @inbounds (h.keys[index], h.vals[index])
    key2 = convert(K, key)::K

    age0 = h.age
    v = default()
    if !isa(v, V)
        v = convert(V, v)::V
    end
    if h.age != age0
        index, sh = Base.ht_keyindex2_shorthash!(h, key2)
    end
    if index > 0
        h.age += 1
        @inbounds h.keys[index] = key2
        @inbounds h.vals[index] = v
    else
        @inbounds Base._setindex!(h, v, key2, -index, sh)
    end
    return (key2, v)
end

function _read_streamed_strings(path, nodes, nodes_task, edges_task)
    io = open(path, "r", lock=false)
    buf = Vector{UInt8}(undef, 4096)
    strmap = sizehint!(Dict{String, UInt32}(), length(nodes) >> 1)
    strings = sizehint!(String[], length(nodes))
    @inbounds try
        while !eof(io)
            str_size = read(io, UInt)
            if str_size > length(buf)
                resize!(buf, str_size)
            end
            bv = @view buf[1:str_size]
            read!(io, bv)
            strv = StringView(bv)

            str, _ = _get!(strmap, strv) do
                UInt32(length(strmap))
            end
            push!(strings, str)
        end
        empty!(buf)
        sizehint!(buf, 0)

        wait(nodes_task)
        for i in 1:length(nodes)
            str = strings[nodes.name_index[i]+true]
            nodes.name_index[i] = strmap[str]
        end

        wait(edges_task)
        for i in 1:length(nodes.edges)
            (nodes.edges.type[i] in (Int8(1), Int8(2))) && continue
            str = strings[nodes.edges.name_index[i]+true]
            nodes.edges.name_index[i] = strmap[str]
        end

        resize!(strings, length(strmap))
        for (k, v) in strmap
            strings[v+true] = k
        end
        empty!(strmap)
        sizehint!(strmap, 0)

        return strings
    finally
        close(io)
    end
end

function _write_snapshot_permute(path, nodes, node_fields, strings, perms)
    open(path, "w") do io
        println(io, """
            {"snapshot":{"meta":{"node_fields":["type","name","id","self_size","edge_count","trace_node_id","detachedness"],"node_types":[["synthetic","jl_task_t","jl_module_t","jl_array_t","object","String","jl_datatype_t","jl_sym_t","jl_svec_t"],"string", "number", "number", "number", "number", "number"],"edge_fields":["type","name_or_index","to_node"],"edge_types":[["internal","hidden","element","property"],"string_or_number","from_node"]},"""
        )
        println(io, "\"node_count\":$(length(nodes)),\"edge_count\":$(length(nodes.edges))},")
        print(io, "\"nodes\":[")
        @inbounds for i in 1:length(nodes)
            i > 1 && println(io, ",")
            _write_decimal_number(io, nodes.type[i])
            print(io, ",")
            _write_decimal_number(io, nodes.name_index[i])
            print(io, ",")
            _write_decimal_number(io, nodes.id[i])
            print(io, ",")
            _write_decimal_number(io, nodes.self_size[i])
            print(io, ",")
            _write_decimal_number(io, nodes.edge_count[i])
            print(io, ",0,0")
        end
        print(io, "\n],\n\"edges\":[")
        _written_init = false
        @inbounds for i in perms
            _written_init && println(io, ",")
            _write_decimal_number(io, nodes.edges.type[i])
            print(io, ",")
            _write_decimal_number(io, nodes.edges.name_index[i])
            print(io, ",")
            _write_decimal_number(io, Int(nodes.edges.to_pos[i]) * length(node_fields))
            _written_init = true
        end
        print(io, "\n],\n\"strings\":[")
        for (i, s) in enumerate(strings)
            i > 1 && println(io, ",")
            JSON3.write(io, s)
        end
        println(io, "\n]}")
    end
end

function assemble_snapshot(in_prefix, out_file=string(in_prefix, ".heapsnapshot"))
    meta = JSON3.read("$in_prefix.metadata.json")

    nodes = Nodes(undef, meta[:snapshot][:node_count], meta[:snapshot][:edge_count])

    nodes_task = Threads.@spawn _read_streamed_nodes("$in_prefix.nodes", $nodes)
    edges_task = Threads.@spawn _read_streamed_edges("$in_prefix.edges", $nodes)

    strings = _read_streamed_strings("$in_prefix.strings", nodes, nodes_task, edges_task)
    perms = _get_permuation_vec(nodes)

    _write_snapshot_permute(out_file, nodes, NODE_FIELDS, strings, perms)
    return out_file
end
