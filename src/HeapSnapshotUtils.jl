module HeapSnapshotUtils

using JSON3
using Mmap
using Parsers
using CodecZlibNG

export subsample_snapshot, assemble_snapshot

# SoA layout to help reduce field padding
struct Edges
    type::Vector{Int8}       # index into `snapshot.meta.edge_types`
    name_index::Vector{UInt} # index into `snapshot.strings`
    to_pos::Vector{UInt32}   # index into `snapshot.nodes`
end
function init_edges(n::Int)
    Edges(
        Vector{Int8}(undef, n),
        Vector{UInt}(undef, n),
        Vector{UInt32}(undef, n),
    )
end
Base.length(n::Edges) = length(n.type)

# trace_node_id and detachedness are always 0 in the snapshots Julia produces so we don't store them
struct Nodes
    type::Vector{Int8}         # index in index into `snapshot.meta.node_types`
    name_index::Vector{UInt32} # index in `snapshot.strings`
    id::Vector{UInt}           # unique id, in julia it is the address of the object
    self_size::Vector{Int}     # size of the object itself, not including the size of its fields
    edge_count::Vector{UInt32} # number of outgoing edges
    edges::Edges               # outgoing edges
end
function init_nodes(n::Int, e::Int)
    Nodes(
        Vector{Int8}(undef, n),
        Vector{UInt32}(undef, n),
        Vector{UInt}(undef, n),
        Vector{Int}(undef, n),
        Vector{UInt32}(undef, n),
        init_edges(e),
    )
end
Base.length(n::Nodes) = length(n.type)

function _parse_nodes_array!(nodes, file, pos, options)
    for i in 1:length(nodes)
        res1 = Parsers.xparse(Int8, file, pos, length(file), options)
        node_type = res1.val
        pos += res1.tlen

        res2 = Parsers.xparse(UInt32, file, pos, length(file), options)
        node_name_index = res2.val
        pos += res2.tlen

        res3 = Parsers.xparse(UInt, file, pos, length(file), options)
        id = res3.val
        pos += res3.tlen

        res4 = Parsers.xparse(Int, file, pos, length(file), options)
        self_size = res4.val
        pos += res4.tlen

        res5 = Parsers.xparse(UInt, file, pos, length(file), options)
        edge_count = res5.val
        pos += res5.tlen

        _res = Parsers.xparse(Int8, file, pos, length(file), options)
        @assert _res.val == 0 (_res, i, pos) # trace_node_id
        pos += _res.tlen
        _res = Parsers.xparse(Int8, file, pos, length(file), options)
        @assert _res.val == 0 (_res, i, pos) # detachedness
        pos += _res.tlen
        pos = last(something(findnext(_parseable, file, pos), pos:pos))

        nodes.type[i] = node_type
        nodes.name_index[i] = node_name_index
        nodes.id[i] = id
        nodes.self_size[i] = self_size
        nodes.edge_count[i] = edge_count
    end
    return pos
end

function _parse_edges_array!(nodes, file, pos, backwards_edges, options)
    index = 0
    n_node_fields = 7
    node_idx = UInt32(1)
    edges = nodes.edges
    for edge_count in nodes.edge_count
        for _ in 1:edge_count
            res1 = Parsers.xparse(Int8, file, pos, length(file), options)
            edge_type = res1.val
            pos += res1.tlen

            res2 = Parsers.xparse(UInt, file, pos, length(file), options)
            edge_name_index = res2.val
            pos += res2.tlen

            res3 = Parsers.xparse(UInt32, file, pos, length(file), options)
            to_node = res3.val
            pos += res3.tlen
            pos = last(something(findnext(_parseable, file, pos), pos:pos))

            idx = div(to_node, n_node_fields) + true # convert an index in the nodes array to a node number

            push!(backwards_edges[idx], node_idx)
            index += 1
            edges.type[index] = edge_type
            edges.name_index[index] = edge_name_index
            edges.to_pos[index] = idx
        end
        node_idx += UInt32(1)
    end
    return pos
end

# In the streaming format, edges contain a fourth column which stores the index of the source node.
# The third column, to_node, is also different as it stores the nodes' number, but the index in the nodes array.
# Nodes always have 0 edge_count so we need to updat them here.
function _parse_and_assebmle_edges_array!(nodes, file, pos, edge_count, options)
    index = 0
    edges = nodes.edges
    for _ in 1:edge_count
        res1 = Parsers.xparse(Int8, file, pos, length(file), options)
        edge_type = res1.val
        pos += res1.tlen

        res2 = Parsers.xparse(UInt, file, pos, length(file), options)
        edge_name_index = res2.val
        pos += res2.tlen

        res3 = Parsers.xparse(UInt32, file, pos, length(file), options)
        to_node = res3.val
        pos += res3.tlen

        res4 = Parsers.xparse(UInt32, file, pos, length(file), options)
        from_node = res4.val
        pos += res4.tlen
        pos = last(something(findnext(_parseable, file, pos), pos:pos))

        index += 1
        edges.type[index] = edge_type
        edges.name_index[index] = edge_name_index
        edges.to_pos[index] = to_node + true
        nodes.edge_count[from_node+1] += 1
    end
    return pos
end

_parseable(b) = !(UInt8(b) in (UInt8(' '), UInt8('\n'), UInt8('\r'), UInt8(',')))
function parse_nodes(path)
    OPTIONS = Parsers.Options(delim=',', stripwhitespace=true, ignoreemptylines=true)

    file = Mmap.mmap(path; grow=false, shared=false)
    pos = last(findfirst(b"edge_count\":", file)) + 1
    res_e = Parsers.xparse(Int, file, pos, length(file), OPTIONS)
    edge_count = res_e.val
    pos = last(findfirst(b"node_count\":", file)) + 1
    res_n = Parsers.xparse(Int, file, pos, length(file), OPTIONS)
    node_count = res_n.val

    pos = last(findnext(b"nodes\":[", file, pos))+1
    pos = last(something(findnext(_parseable, file, pos), pos:pos))
    nodes = init_nodes(node_count, edge_count)
    pos = _parse_nodes_array!(nodes, file, pos, OPTIONS)

    pos = last(findnext(b"edges\":[", file, pos))+1
    pos = last(something(findnext(_parseable, file, pos), pos:pos))
    backwards_edges = @time "backwards_edges" map(x->UInt32[], 1:length(nodes))
    pos = _parse_edges_array!(nodes, file, pos, backwards_edges, OPTIONS)

    pos = last(findnext(b"strings\":", file, pos)) + 1
    pos = last(something(findnext(_parseable, file, pos), pos:pos))
    strings = JSON3.read(view(file, pos:length(file)), Vector{String})

    return nodes, strings, backwards_edges
end

function print_sizes(nodes, strings, node_types)
    let size_by_type = zeros(UInt, length(node_types)),
        count_by_type = zeros(Int, length(node_types))

        total_size = UInt(0)

        for (i, type_idx) in enumerate(nodes.type)
            type_idx += Int8(1)
            self_size = UInt(nodes.self_size[i])
            total_size += self_size
            size_by_type[type_idx] += self_size
            count_by_type[type_idx] += 1
        end
        pad = maximum(length, node_types)
        sizes = join(string.(lpad.(node_types, pad), ": ", Base.format_bytes.(size_by_type), " (", count_by_type, ")"), "\n")
        @info "Snapshot contains $(length(nodes)) nodes, $(length(nodes.edges)) edges, and $(length(strings)) strings.\nTotal size of nodes: $(Base.format_bytes(total_size))\n$sizes"
        return size_by_type
    end
end

struct NodeBitset
    x::BitVector
end
NodeBitset(n::Int) = NodeBitset(falses(n))
Base.in(x::UInt32, n::NodeBitset) = n.x[x]
Base.push!(n::NodeBitset, x::UInt32) = (n.x[x] = true)
# Similar to get!(f, ::Dict{K, Nothing}, ::K)
function Base.get!(f::Function, n::NodeBitset, x::UInt32)
    if n.x[x]
        return nothing
    else
        n.x[x] = true
        f()
        return nothing
    end
end

function _mark_seen_and_enqueue_ancestors!(seen, queue, node_idx, backwards_edges)
    get!(seen, node_idx) do
        for parent_node_idx in backwards_edges[node_idx]
            get!(seen, parent_node_idx) do
                push!(queue, parent_node_idx)
            end
        end
        return nothing
    end
end

function filter_nodes!(nodes, backwards_edges, always_keep_types, always_keep_size, sample_prob)
    seen = NodeBitset(length(nodes))
    node_idxs = UInt32(1):UInt32(length(nodes))

    queue = UInt32[]
    for node_idx in node_idxs
        type = nodes.type[node_idx]
        self_size = nodes.self_size[node_idx]
        if !((type in always_keep_types) || (self_size >= always_keep_size) || (rand() < sample_prob))
            continue
        end
        _mark_seen_and_enqueue_ancestors!(seen, queue, node_idx, backwards_edges)

        while !isempty(queue)
            node_idx = pop!(queue)
            _mark_seen_and_enqueue_ancestors!(seen, queue, node_idx, backwards_edges)
        end
    end

    # Create an array that maps the old node index to the new node index
    new_pos = zeros(UInt32, length(nodes))
    j = UInt32(0)
    for i in node_idxs
        if i in seen
            j += UInt32(1)
            new_pos[i] = j
        end
    end

    # Update the edges array
    i = 0
    j = 0
    edges = nodes.edges
    for node_idx in node_idxs
        edge_count = nodes.edge_count[node_idx]
        if node_idx in seen
            for _ in 1:edge_count
                j += 1
                to_pos = edges.to_pos[j]
                if to_pos in seen
                    i += 1
                    edges.type[i] = edges.type[j]
                    edges.name_index[i] = edges.name_index[j]
                    edges.to_pos[i] = new_pos[to_pos]
                else
                    nodes.edge_count[node_idx] -= 1
                end
            end
        else
            j += edge_count
        end
    end
    resize!(edges.type, i)
    resize!(edges.name_index, i)
    resize!(edges.to_pos, i)

    # Update the nodes array
    j = 0
    for i in node_idxs
        if (i in seen)
            j += 1
            nodes.type[j] = nodes.type[i]
            nodes.name_index[j] = nodes.name_index[i]
            nodes.id[j] = nodes.id[i]
            nodes.self_size[j] = nodes.self_size[i]
            nodes.edge_count[j] = nodes.edge_count[i]
        end
    end
    resize!(nodes.type, j)
    resize!(nodes.name_index, j)
    resize!(nodes.id, j)
    resize!(nodes.self_size, j)
    resize!(nodes.edge_count, j)

    return nodes
end

function filter_strings(filtered_nodes, strings, edge_types_map, trunc_strings_to)
    strmap = Dict{String,Int}()
    new_strings = String[]
    property = edge_types_map["property"]
    for (i, str) in enumerate(filtered_nodes.name_index)
        let s = strings[str+1]
            filtered_nodes.name_index[i] = get!(strmap, s) do
                push!(new_strings, first(s, trunc_strings_to))
                length(new_strings) - 1
            end
        end
    end
    edges = filtered_nodes.edges
    for (e, type) in enumerate(edges.type)
        # Edges pointing to an object use the index field to point to the field
        # name in the parent struct.
        if type == property
            let s = strings[edges.name_index[e]+1]
                edges.name_index[e] = get!(strmap, first(s, trunc_strings_to)) do
                    push!(new_strings, s)
                    length(new_strings) - 1
                end
            end
        end
    end
    return new_strings
end

# adapted from Base.dec, which allocates new string. Not threadsafe by default as we mutate DIGIT_BUFS
let DIGIT_BUFS = zeros(UInt8, ndigits(typemax(UInt))),
    _dec_d100 = UInt16[(0x30 + i % 10) << 0x8 + (0x30 + i ÷ 10) for i = 0:99]
    global _write_decimal_number
    _write_decimal_number(io, x::Integer, a=DIGIT_BUFS) = _write_decimal_number(io, unsigned(x), a)
    function _write_decimal_number(io, x::Unsigned, a=DIGIT_BUFS)
        n = ndigits(x)
        i = n
        @inbounds while i >= 2
            d, r = divrem(x, 0x64)
            d100 = _dec_d100[(r % Int)::Int + 1]
            a[i-1] = d100 % UInt8
            a[i] = (d100 >> 0x8) % UInt8
            x = oftype(x, d)
            i -= 2
        end
        if i > 0
            @inbounds a[i] = 0x30 + (rem(x, 0xa) % UInt8)::UInt8
        end
        write(io, @view a[max(i, 1):n])
    end
end

function write_snapshot(path, new_nodes, node_fields, strings)
    open(path, "w") do io
        println(io, """
            {"snapshot":{"meta":{"node_fields":["type","name","id","self_size","edge_count","trace_node_id","detachedness"],"node_types":[["synthetic","jl_task_t","jl_module_t","jl_array_t","object","String","jl_datatype_t","jl_sym_t","jl_svec_t"],"string", "number", "number", "number", "number", "number"],"edge_fields":["type","name_or_index","to_node"],"edge_types":[["internal","hidden","element","property"],"string_or_number","from_node"]},"""
        )
        println(io, "\"node_count\":$(length(new_nodes)),\"edge_count\":$(length(new_nodes.edges))},")
        println(io, "\"nodes\":[")
        for i in 1:length(new_nodes)
            i > 1 && println(io, ",")
            _write_decimal_number(io, new_nodes.type[i])
            print(io, ",")
            _write_decimal_number(io, new_nodes.name_index[i])
            print(io, ",")
            _write_decimal_number(io, new_nodes.id[i])
            print(io, ",")
            _write_decimal_number(io, new_nodes.self_size[i])
            print(io, ",")
            _write_decimal_number(io, new_nodes.edge_count[i])
            print(io, ",0,0")
        end
        println(io, "],\"edges\":[")
        for i in 1:length(new_nodes.edges)
            i > 1 && println(io, ",")
            _write_decimal_number(io, new_nodes.edges.type[i])
            print(io, ",")
            _write_decimal_number(io, new_nodes.edges.name_index[i])
            print(io, ",")
            _write_decimal_number(io, Int(new_nodes.edges.to_pos[i] - 1) * length(node_fields))
        end
        println(io, "],\"strings\":[")
        for (i, s) in enumerate(strings)
            i > 1 && println(io, ",")
            JSON3.write(io, s)
        end
        println(io, "]}")
    end
end

_default_outpath(in_path) = joinpath(dirname(in_path), string("subsampled_", basename(in_path)))

"""
    subsample_snapshot(in_path, out_path, always_keep_types=(0,), always_keep_size=1024*1024, sample_prob=0.1)

`in_path`: path to the snapshot to subsample
`out_path`: where to write the subsampled snapshot
`always_keep_types`: node types to always keep (indices to `snapshot.meta.node_types`)
`always_keep_size`: nodes with `self_size` >= `always_keep_size` will always be kept
`sample_prob`: probability of keeping a node with `self_size` < `always_keep_size`

The subsampled snapshot will contain all nodes reachable from the nodes that are kept.
"""
function subsample_snapshot(in_path, out_path=_default_outpath(in_path), always_keep_types=(0,), always_keep_size=1024*1024, sample_prob=0.1, trunc_strings_to=512*1024)
    @info "Reading snapshot from $(repr(in_path))"
    nodes, strings, backwards_edges = parse_nodes(in_path)

    node_types = ["synthetic","jl_task_t","jl_module_t","jl_array_t","object","String","jl_datatype_t","jl_svec_t","jl_sym_t"]
    node_fields = ["type","name","id","self_size","edge_count","trace_node_id","detachedness"]
    edge_types = ["internal","hidden","element","property"]
    edge_fields = ["type","name_or_index","to_node"]
    edge_types_map = Dict{String,Int}(str=>i-1 for (i, str) in enumerate(edge_types))

    print_sizes(nodes, strings, node_types)

    filter_nodes!(nodes, backwards_edges, always_keep_types, always_keep_size, sample_prob)

    print_sizes(nodes, strings, node_types)

    new_strings = filter_strings(nodes, strings, edge_types_map, trunc_strings_to)

    @info "Writing snapshot to $(repr(out_path))"
    write_snapshot(out_path, nodes, node_fields, new_strings)
    return nothing
end

# # TODO: Rework this to match https://github.com/JuliaLang/julia/pull/51518
# function _default_assembled_outpath(in_prefix, compress)
#     gz_ext = compress ? ".gz" : ""
#     ext = endswith(in_prefix, ".heapsnapshot") ? gz_ext : ".heapsnapshot$gz_ext"
#     isempty(ext) ? in_prefix : string(in_prefix, ext)
# end


# """
#     assemble_snapshot(in_prefix, out_path=in_prefix)

# `in_prefix` is the shared common prefix of inputs, e.g. for:
#     - "./streaming_snapshot/2023-09-20T23_40_25.462.edges"
#     - "./streaming_snapshot/2023-09-20T23_40_25.462.nodes"
#     - "./streaming_snapshot/2023-09-20T23_40_25.462.strings"
#     - "./streaming_snapshot/2023-09-20T23_40_25.462.json"
#   the `in_prefix` should be "./streaming_snapshot/2023-09-20T23_40_25.462"
# `out_path`: where to write the assembled snapshot.
#      Chrome will complain the file extension is not ".heapsnapshot"
# """
# function assemble_snapshot(in_prefix, out_path=nothing, compress=false)
#     if isnothing(out_path)
#         out_path = _default_assembled_outpath(in_prefix, compress)
#     end
#     @info "Reading snapshot from \"$in_prefix{.json,.nodes,.edges,.strings}\""
#     preamble = JSON3.read(string(in_prefix, ".json"))
#     node_count = preamble.snapshot.node_count
#     edge_count = preamble.snapshot.edge_count
#     node_fields = collect(preamble.snapshot.meta.node_fields)

#     nodes = init_nodes(node_count, edge_count)
#     OPTIONS = Parsers.Options(delim=',', stripwhitespace=true, ignoreemptylines=true)

#     node_file = Mmap.mmap(string(in_prefix, ".nodes"); grow=false, shared=false)
#     _parse_nodes_array!(nodes, node_file, 1, OPTIONS)

#     edges_file = Mmap.mmap(string(in_prefix, ".edges"); grow=false, shared=false)
#     _parse_and_assebmle_edges_array!(nodes, edges_file, 1, edge_count, OPTIONS)

#     strings = JSON3.read(string(in_prefix, ".strings")).strings;

#     @info "Writing snapshot to   $(repr(out_path))"
#     write_snapshot(out_path, nodes, node_fields, strings)
# end

end # module HeapSnapshotUtils
