module HeapSnapshotUtils

using JSON3
using Mmap
using Parsers
using CodecZlibNG

export subsample_snapshot, assemble_snapshot
export check_orphan_nodes, check_orphan_nodes_from_backward_edges, get_backwards_edges

const max_tries_fix_orphans::Int32 = 100 # maximum number of iterations to fix orphan nodes
const uber_root_node_idx::UInt32 = 1 # index of the uber root node

# SoA layout to help reduce field padding
struct Edges
    type::Vector{Int8}       # index into `snapshot.meta.edge_types`
    name_index::Vector{UInt} # index into `snapshot.strings`
    to_pos::Vector{UInt32}   # index into `snapshot.nodes`
end
function Edges(n::Int)
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
function Nodes(n::Int, e::Int)
    Nodes(
        Vector{Int8}(undef, n),
        Vector{UInt32}(undef, n),
        Vector{UInt}(undef, n),
        Vector{Int}(undef, n),
        Vector{UInt32}(undef, n),
        Edges(e),
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
    nodes = Nodes(node_count, edge_count)
    pos = _parse_nodes_array!(nodes, file, pos, OPTIONS)

    pos = last(findnext(b"edges\":[", file, pos))+1
    pos = last(something(findnext(_parseable, file, pos), pos:pos))
    backwards_edges = map(x->UInt32[], 1:length(nodes))
    pos = _parse_edges_array!(nodes, file, pos, backwards_edges, OPTIONS)

    pos = last(findnext(b"strings\":", file, pos)) + 1
    pos = last(something(findnext(_parseable, file, pos), pos:pos))
    strings = JSON3.read(view(file, pos:length(file)), Vector{String})

    return nodes, strings, backwards_edges
end

function print_sizes(prefix, nodes, strings, node_types)
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
        @info "$prefix Snapshot contains $(length(nodes)) nodes, $(length(nodes.edges)) edges, and $(length(strings)) strings.\nTotal size of nodes: $(Base.format_bytes(total_size))\n$sizes"
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

# reset the bit at index x to false if it's true
function reset!(n::NodeBitset, x::UInt32)
    @assert length(n.x) >= x "Invalid node index: $x, length: $(length(n.x))"
    if n.x[x]
        n.x[x] = false
    end
    return nothing
end

function _mark!(seen, queue, node_idx, nodes, cumsum_edges)
    get!(seen, node_idx) do
        cumcnt = cumsum_edges[node_idx]
        prev_cumcnt = get(cumsum_edges, node_idx-1, 0)

        for child_node_idx in @view(nodes.edges.to_pos[prev_cumcnt+1:cumcnt])
            get!(seen, child_node_idx) do
                push!(queue, child_node_idx)
            end
        end
    end
    return nothing
end

# check the nodes that don't have any parents except the uber root node
function check_orphan_nodes_from_backward_edges(backwards_edges)
    orphan_nodes = Set{UInt32}()
    for i in 1:length(backwards_edges)
        if isempty(backwards_edges[i]) && i != uber_root_node_idx
            push!(orphan_nodes, i)
        end
    end
    return orphan_nodes
end

# We need to repair the parents of the nodes we're not filtering out, otherwise the
# snapshot might include some orphan nodes.
function reset_parents!(to_filter_out, orphan_nodes, backwards_edges)
    if isempty(orphan_nodes)
        return to_filter_out
    end

    queue = UInt32[]
    for orphan_node_idx in orphan_nodes
        if orphan_node_idx != uber_root_node_idx && length(backwards_edges[orphan_node_idx]) > 0
            for parent_idx in backwards_edges[orphan_node_idx]
                reset!(to_filter_out, parent_idx)
                push!(queue, parent_idx)
            end
        end
        while !isempty(queue)
            n_idx = pop!(queue)
            if n_idx != uber_root_node_idx && length(backwards_edges[n_idx]) > 0
                for parent_idx in backwards_edges[n_idx]
                    if in(to_filter_out, parent_idx)
                        reset!(to_filter_out, parent_idx)
                        push!(queue, parent_idx)
                    end
                end
            end
        end
    end

    return to_filter_out
end

function get_backwards_edges(nodes)
    backwards_edges = map(x->UInt32[], 1:length(nodes))
    edges = nodes.edges
    edge_idx = 0
    for node_idx in 1:length(nodes)
        edge_count = nodes.edge_count[node_idx]
        for _ in 1:edge_count
            edge_idx += 1
            to_pos = edges.to_pos[edge_idx]
            push!(backwards_edges[to_pos], node_idx)
        end
    end
    return backwards_edges
end

function resize_nodes!(to_filter_out, nodes)
    node_idxs = UInt32(1):UInt32(length(nodes))
    # Create an array that maps the old node index to the new node index
    new_pos = zeros(UInt32, length(nodes))
    new_node_idx = UInt32(0)
    for node_idx in node_idxs
        if !(node_idx in to_filter_out)
            new_node_idx += UInt32(1)
            new_pos[node_idx] = new_node_idx
        end
    end

    # Update the edges array
    new_edge_idx = 0
    edge_idx = 0
    edges = nodes.edges
    for node_idx in node_idxs
        edge_count = nodes.edge_count[node_idx]
        if node_idx in to_filter_out
            # skip over all the edges of the node we're filtering out
            edge_idx += edge_count
        else
            # filter out the edges to the nodes we're filtering out
            for _ in 1:edge_count
                edge_idx += 1
                to_pos = edges.to_pos[edge_idx]
                if to_pos in to_filter_out
                    nodes.edge_count[node_idx] -= 1
                else
                    new_edge_idx += 1
                    edges.type[new_edge_idx] = edges.type[edge_idx]
                    edges.name_index[new_edge_idx] = edges.name_index[edge_idx]
                    edges.to_pos[new_edge_idx] = new_pos[to_pos]
                end
            end
        end
    end
    resize!(edges.type, new_edge_idx)
    resize!(edges.name_index, new_edge_idx)
    resize!(edges.to_pos, new_edge_idx)

    # Update the nodes array
    new_node_idx = 0
    for node_idx in node_idxs
        if !(node_idx in to_filter_out)
            new_node_idx += 1
            nodes.type[new_node_idx] = nodes.type[node_idx]
            nodes.name_index[new_node_idx] = nodes.name_index[node_idx]
            nodes.id[new_node_idx] = nodes.id[node_idx]
            nodes.self_size[new_node_idx] = nodes.self_size[node_idx]
            nodes.edge_count[new_node_idx] = nodes.edge_count[node_idx]
        end
    end
    resize!(nodes.type, new_node_idx)
    resize!(nodes.name_index, new_node_idx)
    resize!(nodes.id, new_node_idx)
    resize!(nodes.self_size, new_node_idx)
    resize!(nodes.edge_count, new_node_idx)

    return nodes
end

function filter_nodes!(f, nodes, strings, backwards_edges)
    to_filter_out = NodeBitset(length(nodes))
    node_idxs = UInt32(1):UInt32(length(nodes))

    cumsum_edges = cumsum(nodes.edge_count)
    queue = UInt32[]
    # filter out the nodes that don't match the condition specified by the function f()
    for node_idx in node_idxs
        node_type = nodes.type[node_idx]
        self_size = nodes.self_size[node_idx]
        node_name = strings[nodes.name_index[node_idx]+1]
        f(node_type, self_size, node_name) && continue

        _mark!(to_filter_out, queue, node_idx, nodes, cumsum_edges)

        while !isempty(queue)
            n_idx = pop!(queue)
            _mark!(to_filter_out, queue, n_idx, nodes, cumsum_edges)
        end
    end

    # the above filtering might result in orphan nodes since we operate on
    # a complex graph structure with object cross references. We need to try
    # to fix the parents of the orphan nodes to avoid orphan nodes in best effort
    orphan_nodes = find_possible_orphan_nodes(to_filter_out, nodes)
    count = 0
    while length(orphan_nodes) > 0
        count += 1
        if count > max_tries_fix_orphans
            @warn "Too many iterations to processing orphan node processing, breaking..."
            break
        end
        @info "Attempting to fix parents for $(length(orphan_nodes)) orphan nodes..."
        cur_to_filter_out = NodeBitset(copy(to_filter_out.x))
        to_filter_out = reset_parents!(to_filter_out, orphan_nodes, backwards_edges)
        # if we didn't change anything, we can break
        if cur_to_filter_out.x == to_filter_out.x
            @info "No changes can be done to fix parents, breaking..."
            break
        end
        orphan_nodes = find_possible_orphan_nodes(to_filter_out, nodes)
    end

    # remove the nodes and related edges that we're filtering out
    new_nodes = resize_nodes!(to_filter_out, nodes)
    return new_nodes
end

# remove the orphan nodes and their children from the nodes array
function remove_possible_orphan_nodes(orphan_nodes, nodes)
    if isempty(orphan_nodes)
        return nodes
    end

    to_filter_out = NodeBitset(length(nodes))
    cumsum_edges = cumsum(nodes.edge_count)
    queue = UInt32[]
    for node_idx in orphan_nodes
        _mark!(to_filter_out, queue, node_idx, nodes, cumsum_edges)

        while !isempty(queue)
            n_idx = pop!(queue)
            _mark!(to_filter_out, queue, n_idx, nodes, cumsum_edges)
        end
    end
    new_nodes = resize_nodes!(to_filter_out, nodes)
    return new_nodes
end

function find_possible_orphan_nodes(to_filter_out, nodes)
    node_idxs = UInt32(1):UInt32(length(nodes))
    orphan_nodes = Set{UInt32}()
    for node_idx in node_idxs
        if !(node_idx in to_filter_out)
            push!(orphan_nodes, node_idx)
        end
    end

    cumsum_edges = cumsum(nodes.edge_count)
    for node_idx in node_idxs
        if !(node_idx in to_filter_out)
            cumcnt = cumsum_edges[node_idx]
            prev_cumcnt = get(cumsum_edges, node_idx-1, 0)
            for child_node_idx in @view(nodes.edges.to_pos[prev_cumcnt+1:cumcnt])
                if child_node_idx in orphan_nodes
                    delete!(orphan_nodes, child_node_idx)
                end
            end
        end
    end

    if uber_root_node_idx in orphan_nodes
         delete!(orphan_nodes, uber_root_node_idx)
    end

    return orphan_nodes
end

function check_orphan_nodes(nodes)
    node_idxs = UInt32(1):UInt32(length(nodes))
    orphan_nodes = Set(node_idxs)
    for idx in nodes.edges.to_pos
        @assert idx in node_idxs
        if idx in orphan_nodes
            delete!(orphan_nodes, idx)
        end
    end
    if uber_root_node_idx in orphan_nodes
         delete!(orphan_nodes, uber_root_node_idx)
    end

    return orphan_nodes
end

function filter_strings(filtered_nodes, strings, trunc_strings_to)
    strmap = Dict{String,Int}()
    new_strings = String[]

    for (i, str) in enumerate(filtered_nodes.name_index)
        let s = strings[str+1]
            filtered_nodes.name_index[i] = get!(strmap, s) do
                push!(new_strings, isnothing(trunc_strings_to) ? s : first(s, trunc_strings_to))
                length(new_strings) - 1
            end
        end
    end
    edges = filtered_nodes.edges
    for (e, type) in enumerate(edges.type)
        # Edges pointing to an object use the index field to point to the field
        # name in the parent struct. Type 2 are edges pointing to an element of
        # an array, so we don't need to update the name index.
        type == 2 && continue
        edge_name_idx = edges.name_index[e]
        let s = strings[edge_name_idx+1]
            edges.name_index[e] = get!(strmap, isnothing(trunc_strings_to) ? s : first(s, trunc_strings_to)) do
                push!(new_strings, s)
                length(new_strings) - 1
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
    subsample_snapshot(f, in_path, out_path; trunc_strings_to=nothing)) -> String

Subsamples a snapshot by filtering out nodes that don't match the predicate `f`.
By default, the subsampled snapshot is written to the same directory as the original snapshot,
with its name prefixed by "subsampled_". Returns the `out_path`.

- `f`: A function used for filtering nodes. It should take the following arguments:
    - `node_type`: index into `snapshot.meta.node_types`
    - `node_size`: size of the object itself in bytes
    - `node_name`: name of the object
- `in_path`: path to the snapshot to subsample
- `out_path`: where to write the subsampled snapshot
- `trunc_strings_to`: if not `nothing`, strings will be truncated to this length

Example:
```julia
subsample_snapshot("profile1.heapsnapshot") do node_type, node_size, node_name
    # node types are 0 based indices into:
    # ["synthetic","jl_task_t","jl_module_t","jl_array_t","object","String","jl_datatype_t","jl_svec_t","jl_sym_t"]
    node_type in (0,1,2,6,7,8) || node_size >= 64 || occursin(r"rel"i, node_name)
end

# Only truncates strings to 512 bytes
subsample_snapshot((x...)->true, "profile1.heapsnapshot", trunc_strings_to=512)
```
"""
function subsample_snapshot(f, in_path, out_path=_default_outpath(in_path); trunc_strings_to=nothing)
    @info "Reading snapshot from $(repr(in_path))"
    nodes, strings, backwards_edges = parse_nodes(in_path)

    orphan_nodes = check_orphan_nodes_from_backward_edges(backwards_edges)
    if !isempty(orphan_nodes)
        @info "Input snapshot file contains $(length(orphan_nodes)) orphan nodes, processing orphan node removal"
        nodes = remove_possible_orphan_nodes(orphan_nodes, nodes)
        backwards_edges = get_backwards_edges(nodes)
    end

    node_types = ["synthetic","jl_task_t","jl_module_t","jl_array_t","object","String","jl_datatype_t","jl_svec_t","jl_sym_t"]
    node_fields = ["type","name","id","self_size","edge_count","trace_node_id","detachedness"]
    edge_types = ["internal","hidden","element","property"]
    edge_fields = ["type","name_or_index","to_node"]

    print_sizes("BEFORE: ", nodes, strings, node_types)

    new_nodes = filter_nodes!(f, nodes, strings, backwards_edges)

    new_strings = filter_strings(nodes, strings, trunc_strings_to)
    orphan_nodes = check_orphan_nodes(new_nodes)
    if !isempty(orphan_nodes)
        @warn "Output snapshot contains $(length(orphan_nodes)) orphan nodes out of $(length(new_nodes)) nodes"
    end

    print_sizes("AFTER:  ", new_nodes, new_strings, node_types)

    @info "Writing snapshot to $(repr(out_path))"
    write_snapshot(out_path, new_nodes, node_fields, new_strings)
    return out_path
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
