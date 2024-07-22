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
            # some name indices are typemax(UInt64), but for no good reason AFAICT, so we just truncate
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

# Only materializes the StringView "key" if it's not already in the dictionary
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
    # These don't have a corresponding name in strings
    elem_type_index = Int8(findfirst(==("element"), nodes.edge_types)::Int - 1)
    hide_type_index = Int8(findfirst(==("hidden"), nodes.edge_types)::Int - 1)

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
            (nodes.edges.type[i] in (elem_type_index, hide_type_index)) && continue
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

function _maybe_write!(io, d, k, isfirst=false)
    if haskey(d, k)
        _pop_write!(io, d, k, isfirst)
        return true
    end
    return false
end
function _pop_write!(io, d, k, isfirst=false)
    isfirst || print(io, ",")
    print(io, "\"$k\":")
    JSON3.write(io, pop!(d, k))
end

function _write_snapshot_permute(path, metadata, nodes, node_fields, strings, perms)
    snapshot = pop!(metadata, :snapshot)
    meta = pop!(snapshot, :meta)
    open(path, "w") do io
        print(io, "{\"snapshot\":{")
            # Trying to write the metadata in the same order as the original
            # the VSCode heap snapshot is very picky about the order of and format of the metadata
            print(io, "\"meta\":{")
                _pop_write!(io, meta, :node_fields, true)
                _pop_write!(io, meta, :node_types)
                _pop_write!(io, meta, :edge_fields)
                _pop_write!(io, meta, :edge_types)
                _maybe_write!(io, meta, :trace_function_info_fields)
                _maybe_write!(io, meta, :trace_node_fields)
                _maybe_write!(io, meta, :sample_fields)
                _maybe_write!(io, meta, :location_fields)
                for k in keys(meta)
                    _pop_write!(io, meta, k)
                end
            println(io, "},")
            _pop_write!(io, snapshot, :node_count, true)
            _pop_write!(io, snapshot, :edge_count)
            _maybe_write!(io, meta, :trace_function_count)
            for k in keys(snapshot)
                _pop_write!(io, snapshot, k)
            end
            println(io, "},")
        _written_meta  = _maybe_write!(io, metadata, :trace_function_infos, true)
        _written_meta |= _maybe_write!(io, metadata, :trace_tree,           !_written_meta)
        _written_meta |= _maybe_write!(io, metadata, :samples,              !_written_meta)
        _written_meta |= _maybe_write!(io, metadata, :locations,            !_written_meta)
        for k in keys(metadata)
            _pop_write!(io, meta, k, _written_meta)
            _written_meta = true
        end
        _written_meta && println(io, ",")

        println(io, "\"nodes\":[")
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

        print(io, "],\"edges\":[\n")
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

        print(io, "],\n\"strings\":[\n")
        for (i, s) in enumerate(strings)
            i > 1 && println(io, ",")
            JSON3.write(io, s)
        end
        println(io, "\n]}")
    end
end

function assemble_snapshot(in_prefix, out_file=endswith(in_prefix, ".heapsnapshot") ? in_prefix : string(in_prefix, ".heapsnapshot"))
    meta = copy(JSON3.read("$in_prefix.metadata.json"))

    nodes = Nodes(
        undef,
        meta[:snapshot][:meta][:node_types][1],
        meta[:snapshot][:meta][:edge_types][1],
        meta[:snapshot][:node_count],
        meta[:snapshot][:edge_count]
    )

    nodes_task = Threads.@spawn _read_streamed_nodes("$in_prefix.nodes", $nodes)
    edges_task = Threads.@spawn _read_streamed_edges("$in_prefix.edges", $nodes)

    strings = _read_streamed_strings("$in_prefix.strings", nodes, nodes_task, edges_task)
    perms = _get_permuation_vec(nodes)

    _write_snapshot_permute(out_file, meta, nodes, NODE_FIELDS, strings, perms)
    return out_file
end
