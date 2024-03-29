module HeapSnapshotUtils

using JSON3
using Mmap
using Parsers
using CodecZlibNG

export subsample_snapshot

# SoA layout to help reduce field padding
struct Edges
    type::Vector{Int8}       # index into `snapshot.meta.edge_types`
    name_index::Vector{UInt} # index into `snapshot.strings`
    to_pos::Vector{UInt32}   # index into `snapshot.nodes`
end
function Edges(::UndefInitializer, n::Int)
    Edges(
        Vector{Int8}(undef, n),
        Vector{UInt}(undef, n),
        Vector{UInt32}(undef, n),
    )
end
Base.length(n::Edges) = length(n.type)

# trace_node_id and detachedness are always 0 in the snapshots Julia produces so we don't store them
struct Nodes
    type::Vector{Int8}          # index in index into `snapshot.meta.node_types`
    name_index::Vector{UInt32}  # index in `snapshot.strings`
    id::Vector{UInt}            # unique id, in julia it is the address of the object
    self_size::Vector{Int}      # size of the object itself, not including the size of its fields
    edge_count::Vector{UInt32}  # number of outgoing edges
    _back_count::Vector{UInt32} # number of inbound edges (not a part of the snapshot format, but useful for filtering nodes)
    edges::Edges                # outgoing edges
end
function Nodes(::UndefInitializer, n::Int, e::Int)
    Nodes(
        Vector{Int8}(undef, n),
        Vector{UInt32}(undef, n),
        Vector{UInt}(undef, n),
        Vector{Int}(undef, n),
        Vector{UInt32}(undef, n),
        zeros(UInt32, n),
        Edges(undef, e),
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

function _parse_edges_array!(nodes, file, pos, options)
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

            index += 1
            edges.type[index] = edge_type
            edges.name_index[index] = edge_name_index
            edges.to_pos[index] = idx
            nodes._back_count[idx] += UInt32(1)
        end
        node_idx += UInt32(1)
    end

    # NOTE: if we ever need to also find the `from_pos` edges, we can do the following:
    # cumback = Vector{UInt32}(undef, length(nodes))
    # cumback = cumsum!(cumback, nodes._back_count)
    # fill!(nodes._back_count, 0)

    # index = 0
    # node_idx = UInt32(0)
    # for edge_count in nodes.edge_count
    #     node_idx += UInt32(1)
    #     for _ in 1:edge_count
    #         index += 1
    #         idx = edges.to_pos[index]
    #         prev_cumcnt = get(cumback, idx-1, 0)

    #         edges.from_pos[prev_cumcnt + (nodes._back_count[idx] += UInt32(1))] = node_idx
    #     end
    # end
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
    nodes = Nodes(undef, node_count, edge_count)
    pos = _parse_nodes_array!(nodes, file, pos, OPTIONS)

    pos = last(findnext(b"edges\":[", file, pos))+1
    pos = last(something(findnext(_parseable, file, pos), pos:pos))
    pos = _parse_edges_array!(nodes, file, pos, OPTIONS)

    pos = last(findnext(b"strings\":", file, pos)) + 1
    pos = last(something(findnext(_parseable, file, pos), pos:pos))
    strings = JSON3.read(view(file, pos:length(file)), Vector{String})

    return nodes, strings
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

function _mark!(seen, queue, node_idx, nodes, cumsum_edges)
    get!(seen, node_idx) do
        cumcnt = cumsum_edges[node_idx]
        prev_cumcnt = get(cumsum_edges, node_idx-1, 0)

        for child_node_idx in @view(nodes.edges.to_pos[prev_cumcnt+1:cumcnt])
            if !(child_node_idx in seen)
                new_back = nodes._back_count[child_node_idx] -= 1
                if iszero(new_back) # Is there no longer a path from the root to this child node?
                    if nodes.edge_count[child_node_idx] > 0 # Are there descendants we need to remove?
                        push!(queue, child_node_idx)
                    else
                        push!(seen, child_node_idx)
                    end
                end
            end
        end
        return nothing
    end
end

function filter_nodes!(f, nodes, strings)
    to_filter_out = NodeBitset(length(nodes))
    node_idxs = UInt32(1):UInt32(length(nodes))

    cumsum_edges = cumsum(nodes.edge_count)
    queue = sizehint!(UInt32[], length(nodes)) # we later reuse this array to re-map node indices
    for node_idx in Iterators.rest(node_idxs, 1) # never attemp to filter out the root
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

    # Re-maps the old node index to the new node index
    new_pos = resize!(queue, length(nodes))
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

    # Update the nodes array (we don't bother updating the _back_count array as it is not needed anymore)
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

_trunc_string(s, n) = ncodeunits(s) > n ? first(s, n) : s
_trunc_string(s, ::Nothing) = s

function filter_strings(filtered_nodes, strings, trunc_strings_to)
    strmap = Dict{String,Int}()
    new_strings = String[]

    for (i, str) in enumerate(filtered_nodes.name_index)
        let s = strings[str+1]
            filtered_nodes.name_index[i] = get!(strmap, s) do
                push!(new_strings, _trunc_string(s, trunc_strings_to))
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
            edges.name_index[e] = get!(strmap, _trunc_string(s, trunc_strings_to)) do
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
    nodes, strings = parse_nodes(in_path)

    node_types = ["synthetic","jl_task_t","jl_module_t","jl_array_t","object","String","jl_datatype_t","jl_svec_t","jl_sym_t"]
    node_fields = ["type","name","id","self_size","edge_count","trace_node_id","detachedness"]
    edge_types = ["internal","hidden","element","property"]
    edge_fields = ["type","name_or_index","to_node"]

    print_sizes("BEFORE: ", nodes, strings, node_types)

    filter_nodes!(f, nodes, strings)

    new_strings = filter_strings(nodes, strings, trunc_strings_to)

    print_sizes("AFTER:  ", nodes, new_strings, node_types)

    @info "Writing snapshot to $(repr(out_path))"
    write_snapshot(out_path, nodes, node_fields, new_strings)
    return out_path
end

end # module HeapSnapshotUtils
